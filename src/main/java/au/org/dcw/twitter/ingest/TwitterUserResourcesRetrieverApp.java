/*
 * Copyright 2016 Derek Weber
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.org.dcw.twitter.ingest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

import twitter4j.Paging;
import twitter4j.RateLimitStatus;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Application that collects resources associated with Twitter users,
 * such as the most recent tweets, most recent favourites, friend and
 * follower IDs.
 * <p>
 *
 * @see <a href=
 *      "https://github.com/yusuke/twitter4j/blob/master/twitter4j-examples/src/main/java/twitter4j/examples/json/SaveRawJSON.java">SaveRawJSON.java</a>
 * @see <a href=
 *      "https://dev.twitter.com/rest/reference/get/statuses/user_timeline">Twitter's
 *      <code>GET status/user_timeline</code> endpoint for tweets</a>
 */
@SuppressWarnings("static-access")
public final class TwitterUserResourcesRetrieverApp {
    private static final String FILE_SEPARATOR = System.getProperty("file.separator", "/");
    /**
     * This fudge amount (milliseconds) is added to the time a thread goes to
     * sleep when it needs to wait until the next rate limiting window. This is
     * to guard against clock skew between the local clock and Twitter's clock,
     * and also the inherent jitter in Thread.sleep(). 10 seconds seems like a
     * reasonable amount.
     */
    private static final int SLEEP_DURATION_FUDGE_AMOUNT = 10000;
//    private static final String USER_AGENT = "Mozilla/5.0";

    private static final String GET_TWEETS = "/statuses/home_timeline";
    private static final String GET_FAVES = "/favorites/list";
    private static final String GET_FOLLOWERS = "/followers/ids";
    private static final String GET_FRIENDS = "/friends/ids";

    private static Logger LOG = LoggerFactory.getLogger(TwitterUserResourcesRetrieverApp.class);
//    private static final int FETCH_BATCH_SIZE = 100;

    static class Config {
        private static final Options OPTIONS = new Options();
        static {
            OPTIONS.addOption("i", "identifiers-file", true, "File of Twitter screen names");
            OPTIONS.addOption(longOpt("tweets", "Collect statuses (tweets)").create());
            OPTIONS.addOption(longOpt("favourites", "Collect favourites").create());
            OPTIONS.addOption(longOpt("followers", "Collect follower IDs").create());
            OPTIONS.addOption(longOpt("friends", "Collect friend (followee) IDs").create());
            OPTIONS.addOption("o", "output-directory", true, "Directory to which to write profiles (default: ./output)");
            OPTIONS.addOption("c", "credentials", true, "File of Twitter credentials (default: ./twitter.properties)");
            OPTIONS.addOption("d", "debug", false, "Turn on debugging information (default: false)");
            OPTIONS.addOption("?", "help", false, "Ask for help with using this tool.");
        }

        private static OptionBuilder longOpt(String name, String description) {
            return OptionBuilder.withLongOpt(name).withDescription(description);
        }

        /**
         * Prints how the app ought to be used and causes the VM to exit.
         */
        private static void printUsageAndExit() {
            new HelpFormatter().printHelp("TwitterUserResourcesRetrieverApp", OPTIONS);
            System.exit(0);
        }

        String identifiersFile = null;
        boolean expectIDs = false;
        boolean collectTweets = false;
        boolean collectFaves = false;
        boolean collectFollowers = false;
        boolean collectFriends = false;
        String outputDir = "./output";
        String credentialsFile = "./twitter.properties";
        boolean debug = false;

        static Config parse(String[] args) {
            final CommandLineParser parser = new BasicParser();
            Config cfg = new Config();
            try {
                final CommandLine cmd = parser.parse(OPTIONS, args);
                if (cmd.hasOption('i')) cfg.identifiersFile = cmd.getOptionValue('i');
                if (cmd.hasOption("tweets")) cfg.collectTweets = true; // 200
                if (cmd.hasOption("favourites")) cfg.collectFaves = true; // nom 200
                if (cmd.hasOption("followers")) cfg.collectFollowers = true;
                if (cmd.hasOption("friends")) cfg.collectFriends = true;
                if (cmd.hasOption('o')) cfg.outputDir = cmd.getOptionValue('o');
                if (cmd.hasOption('c')) cfg.credentialsFile = cmd.getOptionValue('c');
                if (cmd.hasOption('d')) cfg.debug = true;
                if (cmd.hasOption('h')) printUsageAndExit();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return cfg;
        }

        public void check() {
            if (this.debug) {
                System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
            }
            if (this.identifiersFile == null) {
                printUsageAndExit();
            }
        }
    }

    /**
     * Encapsulates the holy trinity of Twitter API rate limit data.
     */
    public class LimitData {

        /**
         * The maximum call limit per limiting window.
         */
        public int limit;

        /**
         * The remaining call limit in the current limiting window.
         */
        public int remaining;

        /**
         * The timestamp, as seconds offset in modern Unix era, at which the next limiting window starts.
         */
        public long reset;

        public LimitData(final int limit, final int remaining, final long reset) {
            this.limit = limit;
            this.remaining = remaining;
            this.reset = reset;
        }

        @Override
        public String toString() {
            return "[" + this.remaining + "/" + this.limit + "/" + this.reset + "]";
        }
    }

    public static void main(String[] args) throws IOException, TwitterException {
        Config cfg = Config.parse(args);
        cfg.check();

        new TwitterUserResourcesRetrieverApp(cfg).run();
    }

    /**
     * For each API end point, the mutex controlling access to the path. Keys of this map are are a function of end point paths; see
     * {@link #endpointPathToLimitKey(String)}.
     */
    public Map<String, ReentrantLock> limitsMutex;

    /**
     * Map from API end point to limit data for that endpoint. Keys of this map are are a function of end point paths; see
     * {@link #endpointPathToLimitKey(String)}.
     */
    public Map<String, LimitData> limits;
//    private CloseableHttpClient httpClient;
    private final Config cfg;
    private final ObjectMapper json;
    private Twitter twitter;

    @FunctionalInterface
    interface ApiCall<Arg1, RetType> {
        public RetType apply(Arg1 arg1);
    }


    /**
     * Constructor.
     *
     * @param cfg Config instance with all required configuration.
     */
    public TwitterUserResourcesRetrieverApp(final Config cfg) {
        this.cfg = cfg;
        this.json = new ObjectMapper();
    }


    /**
     * Fetch the profiles specified by the screen names in the given {@code screenNamesFile}
     * and write the profiles to the given {@code outputDir}.
     *
     * @throws IOException If an error occurs reading files or talking to the network
     * @throws TwitterException If an error occurs haggling with Twitter
     */
    public void run() throws IOException, TwitterException {

        LOG.info("Collecting profiles");
        LOG.info("  Twitter identifiers: " + this.cfg.identifiersFile);
        LOG.info("  output directory: " + this.cfg.outputDir);
        LOG.info("  Collecting tweets:     " + this.cfg.collectTweets);
        LOG.info("  Collecting favourites: " + this.cfg.collectFaves);
        LOG.info("  Collecting followers:  " + this.cfg.collectFollowers);
        LOG.info("  Collecting friends:    " + this.cfg.collectFriends);

//        this.httpClient =
//            HttpClientBuilder.create().setSSLContext(this.setupSSLCertificates()).build();

        final List<String> ids = this.loadIDs(this.cfg.identifiersFile);

        LOG.info("Read {} Twitter IDs", ids.size());

        Stopwatch timer = Stopwatch.createStarted();

        if (!Files.exists(Paths.get(this.cfg.outputDir))) {
            LOG.info("Creating output directory {}", this.cfg.outputDir);
            Paths.get(this.cfg.outputDir).toFile().mkdirs();
        }

        final Configuration config =
            TwitterUserResourcesRetrieverApp.buildTwitterConfiguration(this.cfg.credentialsFile, this.cfg.debug);
        this.twitter = new TwitterFactory(config).getInstance();

        this.retrieveRateLimits(this.twitter);

        for (final String userId : ids) {

            try {
                LOG.info("Looking up {}'s resources", userId);
                final long idAsLong = Long.parseLong(userId);

                // Retrieve tweets
                if (this.cfg.collectTweets) {
                    this.makeApiCall(idAsLong, GET_TWEETS, this.createGetTweetsCallback());
                }
                // Retrieve favourites
                if (this.cfg.collectFaves) {
                    this.makeApiCall(idAsLong, GET_FAVES, this.createGetFavesCallback());
                }
                // Retrieve followers
                if (this.cfg.collectFollowers) {
                    LOG.info("Collecting followers of #{} with {} - NYI", userId, GET_FOLLOWERS);
//                    this.makeApiCall(idAsLong, GET_FOLLOWERS, this.createGetFollowersCallback());
                }
                // Retrieve friends
                if (this.cfg.collectFriends) {
                    LOG.info("Collecting accounts followed by #{} with {} - NYI", userId, GET_FRIENDS);
                }

            } catch (InterruptedException e) {
                LOG.warn("Failed to communicate with Twitter somehow", e);
            }
        }
        LOG.info("Retrieval complete in {} seconds.", timer.elapsed(TimeUnit.SECONDS));
    }


    private ApiCall<Long, RateLimitStatus> createGetTweetsCallback() {
        ApiCall<Long, RateLimitStatus> callback = (Long id) -> {
            try {
                LOG.info("Collecting tweets posted by #{} with {}", id, GET_TWEETS);
                final ResponseList<Status> userTimeline =
                    this.twitter.getUserTimeline(id.longValue(), new Paging(1, 200));
                // extract raw json and write it to file
                final String rawJsonTweets = TwitterObjectFactory.getRawJSON(userTimeline);
                final StringBuilder tweetsToSave = new StringBuilder();
                int i = 0;
                for (JsonNode tweetNode : this.json.readTree(rawJsonTweets)) {
                    i++;
                    tweetsToSave.append(tweetNode.toString()).append('\n');
                }
                String tweetsFile = this.outFile(id.toString() + "-tweets.json");
                saveText(tweetsToSave.toString(), tweetsFile);
                LOG.info("Wrote {} tweets to {}", i, tweetsFile);
                return userTimeline.getRateLimitStatus();
            } catch (IOException | TwitterException e) {
                LOG.warn("Barfed parsing JSON or saving to file tweets for {}.", id, e);
            }
            return null;
        };
        return callback;
    }


    private ApiCall<Long, RateLimitStatus> createGetFavesCallback() {
        ApiCall<Long, RateLimitStatus> callback = (Long id) -> {
            try {
                LOG.info("Collecting tweets favourited by #{} with {}", id, GET_FAVES);
                final ResponseList<Status> favesTimeline =
                    this.twitter.getFavorites(id.longValue(), new Paging(1, 200));
                // extract raw json and write it to file
                final String rawJsonTweets = TwitterObjectFactory.getRawJSON(favesTimeline);
                final StringBuilder favesToSave = new StringBuilder();
                int i = 0;
                for (JsonNode tweetNode : this.json.readTree(rawJsonTweets)) {
                    i++;
                    favesToSave.append(tweetNode.toString()).append('\n');
                }
                String favesFile = this.outFile(id.toString() + "-favourites.json");
                saveText(favesToSave.toString(), favesFile);
                LOG.info("Wrote {} favourites to {}", i, favesFile);
                return favesTimeline.getRateLimitStatus();
            } catch (IOException | TwitterException e) {
                LOG.warn("Barfed parsing JSON or saving to file favourites for {}.", id, e);
            }
            return null;
        };
        return callback;
    }


//    private ApiCall<Long, RateLimitStatus> createGetFollowersCallback() {
//        ApiCall<Long, RateLimitStatus> callback = (Long id) -> {
//            try {
//                LOG.info("Collecting followers of account {} with {}", id, GET_FOLLOWERS);
//                final ResponseList<Status> userTimeline =
//                    this.twitter.getFavorites(id.longValue(), new Paging(1, 200));
//                // extract raw json and write it to file
//                final String rawJsonTweets = TwitterObjectFactory.getRawJSON(userTimeline);
//                final StringBuilder favesToSave = new StringBuilder();
//                int i = 0;
//                for (JsonNode tweetNode : this.json.readTree(rawJsonTweets)) {
//                    i++;
//                    favesToSave.append(tweetNode.toString()).append('\n');
//                }
//                String favesFile = this.outFile(id.toString() + "-followers.json");
//                saveText(favesToSave.toString(), favesFile);
//                LOG.info("Wrote {} favourites to {}", i, favesFile);
//                return userTimeline.getRateLimitStatus();
//            } catch (IOException | TwitterException e) {
//                LOG.warn("Barfed parsing JSON or saving to file favourites for {}.", id, e);
//            }
//            return null;
//        };
//        return callback;
//    }


    /**
     * Invokes the Twitter <code>endpoint</code> with the given
     * <code>twitterId</code> and <code>callback</code> taking into
     * account Twitter rate limits.
     *
     * @param twitterId The Twitter ID argument for the endpoint.
     * @param endpoint The Twitter RESTful endpoint being invoked.
     * @param callback The callback to execute when the endpoint invocation returns.
     * @throws InterruptedException If we're interrupted while awaiting a response.
     */
    private void makeApiCall(
        final long twitterId,
        final String endpoint,
        final ApiCall<Long, RateLimitStatus> callback
    ) throws InterruptedException {
        LOG.debug("Acquiring mutex for limit key {}", endpoint);
        this.limitsMutex.get(endpoint).lockInterruptibly();
        try {
            LOG.debug("Acquired mutex for limit key {}", endpoint);
            LimitData limitData = this.limits.get(endpoint);
            LOG.debug("There are {} remaining calls to {}", limitData.remaining, endpoint);
            if (limitData.remaining == 0) {
                // Fudge added to the advertised reset time to guard against local sleep duration
                // jitter and clock skew between local clock and Twitter's clock.
                long sleepDurationMillis =
                    limitData.reset * 1000 - System.currentTimeMillis() + SLEEP_DURATION_FUDGE_AMOUNT;
                if (sleepDurationMillis > 0) {
                    LOG.debug(
                        "Sleeping {} seconds to get past reset timestamp {}.",
                        sleepDurationMillis / 1000f,
                        this.formatNicely(limitData.reset)
                    );
                    Thread.sleep(sleepDurationMillis);
                    LOG.debug("And I'm back.");
                } else {
                    LOG.debug(
                        "But the limit for {} will have reset by now : reset timestamp is {}.",
                        endpoint, this.formatNicely(limitData.reset)
                    );
                }
            }

            RateLimitStatus rls = callback.apply(twitterId);

            if (rls != null) {
                limitData.remaining = rls.getRemaining();
                LOG.debug("Endpoint {} has {} calls remaining.", endpoint, limitData.remaining);
                limitData.reset = rls.getResetTimeInSeconds();
                LOG.debug("Endpoint {} has a new reset time: {}.", endpoint, limitData.reset);
            }

        } finally {
            this.limitsMutex.get(endpoint).unlock();
            LOG.debug("Released mutex for limit key {}", endpoint);
        }
    }

    private String outFile(String filename) {
        return this.cfg.outputDir + FILE_SEPARATOR + filename;
    }

//    private String screenNameToID(final String screenName) {
//        String id = screenName;
//        try {
////            HttpPost post = new HttpPost("https://75.119.200.192/ajax.php");
//            HttpPost post = new HttpPost("https://tweeterid.com/ajax.php");
//            post.setHeader("User-Agent", this.USER_AGENT);
//            List<NameValuePair> formparams = new ArrayList<>();
//            formparams.add(new BasicNameValuePair("input", screenName));
//            post.setEntity(new UrlEncodedFormEntity(formparams, Consts.UTF_8));
//
//            CloseableHttpResponse response = this.httpClient.execute(post);
//            try {
//                LOG.info("Status: {}", response.getStatusLine().getStatusCode());
//                LOG.info("Reason: {}", response.getStatusLine().getReasonPhrase());
//                HttpEntity entity = response.getEntity();
//                if (entity != null) {
//                    long len = entity.getContentLength();
//                    if (len != -1 && len < 2048) {
//                        id = EntityUtils.toString(entity);
//                        System.out.println("Resolved @" + screenName + " -> " + id);
//                    } else {
//                        System.err.println("Error converting screen name to ID: Error in response");
//                        EntityUtils.consume(entity);
//                        System.err.println("=> " + EntityUtils.toString(entity));
//                    }
//                }
//            } finally {
//                response.close();
//            }
//        } catch (IOException e) {
//            LOG.warn("Error converting screen name to ID", e);
//        }
//        return id;
//    }


//    /**
//     * A forgiving SSL setup is required for talking to https://tweeterid.com/
//     * to do reverse-lookups of user IDs to follow.
//     * <p>
//     *
//     * @see <a href=
//     *      "http://stackoverflow.com/questions/1828775/how-to-handle-invalid-ssl-certificates-with-apache-httpclient">Props
//     *      to this post for how to do this.</a>
//     * @return A forgiving SSL context
//     */
//    private SSLContext setupSSLCertificates() {
//        // configure the SSLContext with a TrustManager
//        SSLContext ctx = null;
//        try {
//            ctx = SSLContext.getInstance("TLS");
//            ctx.init(new KeyManager[0], new TrustManager[] { new DefaultTrustManager() },
//                     new SecureRandom());
//            SSLContext.setDefault(ctx);
//        } catch (NoSuchAlgorithmException | KeyManagementException e) {
//            System.err.println("Error setting SSL up: " + e.getMessage());
//            e.printStackTrace();
//        }
//        return ctx;
//    }


//    /**
//     * Forgiving trust manager for talking to https://...
//     */
//    private static class DefaultTrustManager implements X509TrustManager {
//
//        @Override
//        public void checkClientTrusted(X509Certificate[] arg0, String arg1)
//            throws CertificateException {
//        }
//
//        @Override
//        public void checkServerTrusted(X509Certificate[] arg0, String arg1)
//            throws CertificateException {
//        }
//
//        @Override
//        public X509Certificate[] getAcceptedIssuers() {
//            return null;
//        }
//    }


    private String formatNicely(long epochSecond) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
            ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneId.systemDefault())
        );
    }


    private void retrieveRateLimits(Twitter twitter) throws TwitterException {
        this.limitsMutex = Maps.newConcurrentMap();
        this.limits = Maps.newConcurrentMap();
        Map<String, RateLimitStatus> rateLimitStatuses = twitter.getRateLimitStatus(
            "statuses",  // for "/statuses/user_timeline",
            "favorites", // for "/favorites/list",
            "followers", // for "/followers/ids",
            "friends"    // for "/friends/ids"
        );
        rateLimitStatuses.forEach((endpoint, rls) -> {
            this.limitsMutex.put(endpoint, new ReentrantLock(true));
            this.limits.put(endpoint, new LimitData(rls.getLimit(), rls.getRemaining(), rls.getResetTimeInSeconds()));
            LOG.debug("Resource: {} -> {}", endpoint, this.limits.get(endpoint));
        });


    }

    /**
     * Writes the given {@code text} {@link String} to the specified file.
     *
     * @param text the String to persist (may be JSON)
     * @param fileName the file (including path) to which to write the text
     * @throws IOException if there's a problem writing to the specified file
     */
    private static void saveText(final String text, final String fileName) throws IOException {
        try (final FileOutputStream fos = new FileOutputStream(fileName);
             final OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
             final BufferedWriter bw = new BufferedWriter(osw)) {
            bw.write(text);
            bw.flush();
        }
    }

    private List<String> loadIDs(final String idsFile) throws IOException {
        return Files.readAllLines(Paths.get(idsFile)).stream()
            .map(l -> l.split("#")[0].trim())
            .filter(l -> l.length() > 0 && ! l.startsWith("#"))
//            .map(sn -> (sn.startsWith("@") ? sn.substring(1): sn))
            .collect(Collectors.toList());
    }

    /**
     * Builds the {@link Configuration} object with which to connect to Twitter,
     * including credentials and proxy information if it's specified.
     *
     * @param credentialsFile Property file of Twitter credentials.
     * @param debug Debug flag to pass to Twitter4j's config builder.
     * @return a Twitter4j {@link Configuration} object
     * @throws IOException if there's an error loading the application's
     *         {@link #credentialsFile}.
     */
    private static Configuration buildTwitterConfiguration
        (final String credentialsFile, final boolean debug) throws IOException {
        // TODO find a better name than credentials, given it might contain
        // proxy info
        final Properties credentials = loadCredentials(credentialsFile);

        final ConfigurationBuilder conf = new ConfigurationBuilder();
        conf.setJSONStoreEnabled(true).setDebugEnabled(debug)
            .setOAuthConsumerKey(credentials.getProperty("oauth.consumerKey"))
            .setOAuthConsumerSecret(credentials.getProperty("oauth.consumerSecret"))
            .setOAuthAccessToken(credentials.getProperty("oauth.accessToken"))
            .setOAuthAccessTokenSecret(credentials.getProperty("oauth.accessTokenSecret"));

        final Properties proxies = loadProxyProperties();
        if (proxies.containsKey("http.proxyHost")) {
            conf.setHttpProxyHost(proxies.getProperty("http.proxyHost"))
                .setHttpProxyPort(Integer.parseInt(proxies.getProperty("http.proxyPort")))
                .setHttpProxyUser(proxies.getProperty("http.proxyUser"))
                .setHttpProxyPassword(proxies.getProperty("http.proxyPassword"));
        }

        return conf.build();
    }

    /**
     * Loads the given {@code credentialsFile} from disk.
     *
     * @param credentialsFile the properties file with the Twitter credentials
     * @return A {@link Properties} map with the contents of credentialsFile
     * @throws IOException if there's a problem reading the credentialsFile.
     */
    private static Properties loadCredentials(final String credentialsFile) throws IOException {
        final Properties properties = new Properties();
        properties.load(Files.newBufferedReader(Paths.get(credentialsFile)));
        return properties;
    }

    /**
     * Loads proxy properties from {@code ./proxy.properties} and, if a password
     * is not supplied, asks for it in the console.
     *
     * @return A Properties instance filled with proxy information.
     */
    private static Properties loadProxyProperties() {
        final Properties properties = new Properties();
        final String proxyFile = "./proxy.properties";
        if (new File(proxyFile).exists()) {
            boolean success = true;
            try (Reader fileReader = Files.newBufferedReader(Paths.get(proxyFile))) {
                properties.load(fileReader);
            } catch (final IOException e) {
                System.err.printf("Attempted and failed to load %s: %s\n", proxyFile, e.getMessage());
                success = false;
            }
            if (success && !properties.containsKey("http.proxyPassword")) {
                final String message = "Please type in your proxy password: ";
                final char[] password = System.console().readPassword(message);
                properties.setProperty("http.proxyPassword", new String(password));
                properties.setProperty("https.proxyPassword", new String(password));
            }
            properties.forEach((k, v) -> System.setProperty(k.toString(), v.toString()));
        }
        return properties;
    }
}
