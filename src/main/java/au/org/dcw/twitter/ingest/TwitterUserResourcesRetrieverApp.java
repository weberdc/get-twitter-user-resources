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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

import com.google.common.collect.Maps;

import twitter4j.RateLimitStatus;
import twitter4j.RateLimitStatusEvent;
import twitter4j.RateLimitStatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Application that collects the profiles of Twitter users.
 * <p>
 *
 * @see <a href=
 *      "https://github.com/yusuke/twitter4j/blob/master/twitter4j-examples/src/main/java/twitter4j/examples/json/SaveRawJSON.java">SaveRawJSON.java</a>
 * @see <a href=
 *      "https://dev.twitter.com/rest/reference/get/statuses/user_timeline">Twitter's
 *      <code>GET status/user_timeline</code> endpoint</a>
 */
@SuppressWarnings("static-access")
public final class TwitterUserResourcesRetrieverApp {
    private static Logger LOG = LoggerFactory.getLogger(TwitterUserResourcesRetrieverApp.class);
    private static final int FETCH_BATCH_SIZE = 100;

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
            return "[" + remaining + "/" + limit + "/" + reset + "]";
        }
    }

    public static void main(String[] args) throws IOException, TwitterException {
        Config cfg = Config.parse(args);
        cfg.check();

        new TwitterUserResourcesRetrieverApp().run(cfg);
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

    /**
     * Fetch the profiles specified by the screen names in the given {@code screenNamesFile}
     * and write the profiles to the given {@code outputDir}.
     *
     * @param cfg Config instance with all required configuration.
     * @throws IOException If an error occurs reading files or talking to the network
     * @throws TwitterException If an error occurs haggling with Twitter
     */
    public void run(final Config cfg) throws IOException, TwitterException {

        LOG.info("Collecting profiles");
        LOG.info("  Twitter identifiers: " + cfg.identifiersFile);
        LOG.info("  output directory: " + cfg.outputDir);

        final List<String> screenNames = this.loadScreenNames(cfg.identifiersFile);

        LOG.info("Read {} screen names", screenNames.size());

        if (!Files.exists(Paths.get(cfg.outputDir))) {
            LOG.info("Creating output directory {}", cfg.outputDir);
            Paths.get(cfg.outputDir).toFile().mkdirs();
        }

        final Configuration config =
            TwitterUserResourcesRetrieverApp.buildTwitterConfiguration(cfg.credentialsFile, cfg.debug);
        final Twitter twitter = new TwitterFactory(config).getInstance();

        retrieveRateLimits(twitter);

//        twitter.addRateLimitStatusListener(this.rateLimitStatusListener);
//        final ObjectMapper json = new ObjectMapper();
//
//        for (final List<String> batch : Lists.partition(screenNames, FETCH_BATCH_SIZE)) {
//
//            try {
//                LOG.info("Looking up {} users' profiles", batch.size());
//
//                // ask Twitter for profiles
//                final String[] allocation = new String[batch.size()];
//                final ResponseList<User> profiles = twitter.lookupUsers(batch.toArray(allocation));
//
//                // extract the raw JSON
//                final String rawJsonProfiles = TwitterObjectFactory.getRawJSON(profiles);
//
//                // traverse the raw JSON (rather than the Twitter4j structures)
//                final JsonNode profilesJsonNode = json.readTree(rawJsonProfiles);
//                profilesJsonNode.forEach (profileNode ->  {
//
//                    // extract vars for logging and filename creation
//                    final String profileId = profileNode.get("id_str").asText();
//                    final String screenName = profileNode.get("screen_name").asText();
//
//                    final String fileName = outputDir + "/profile-" + profileId + ".json";
//
//                    LOG.info("Profile @{} {} -> {}", screenName, profileId, fileName);
//                    try {
//                        saveJSON(profileNode.toString(), fileName);
//                    } catch (IOException e) {
//                        LOG.warn("Failed to write to {}", fileName, e);
//                    }
//                });
//
//            } catch (TwitterException e) {
//                LOG.warn("Failed to communicate with Twitter", e);
//            }
//        }
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
            System.out.println("Resource: " + endpoint + " -> " + rls.toString());
            limitsMutex.put(endpoint, new ReentrantLock(true));
            limits.put(endpoint, new LimitData(rls.getLimit(), rls.getRemaining(), rls.getResetTimeInSeconds()));
        });


    }

    /**
     * Writes the given {@code rawJSON} {@link String} to the specified file.
     *
     * @param rawJSON  the JSON String to persist
     * @param fileName the file (including path) to which to write the JSON
     * @throws IOException if there's a problem writing to the specified file
     */
    private static void saveJSON(final String rawJSON, final String fileName) throws IOException {
        try (final FileOutputStream fos = new FileOutputStream(fileName);
             final OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
             final BufferedWriter bw = new BufferedWriter(osw)) {
            bw.write(rawJSON);
            bw.flush();
        }
    }

    private List<String> loadScreenNames(final String screenNamesFile) throws IOException {
        return Files.readAllLines(Paths.get(screenNamesFile)).stream()
            .map(l -> l.trim())
            .filter(l -> l.length() > 0 && ! l.startsWith("#"))
            .map(sn -> (sn.startsWith("@") ? sn.substring(1): sn))
            .collect(Collectors.toList());
    }

    /**
     * Listener to pay attention to when the Twitter's rate limit is being approached or breached.
     */
    final RateLimitStatusListener rateLimitStatusListener = new RateLimitStatusListener() {
        @Override
        public void onRateLimitStatus(final RateLimitStatusEvent event) {
            TwitterUserResourcesRetrieverApp.this.pauseIfNecessary(event.getRateLimitStatus());
        }

        @Override
        public void onRateLimitReached(final RateLimitStatusEvent event) {
            TwitterUserResourcesRetrieverApp.this.pauseIfNecessary(event.getRateLimitStatus());
        }
    };

    /**
     * If the provided {@link RateLimitStatus} indicates that we are about to break the rate
     * limit, in terms of number of calls or time window, then sleep for the rest of the period.
     *
     * @param status The current status of the our calls to Twitter
     */
    protected void pauseIfNecessary(final RateLimitStatus status) {
        if (status == null) {
            return;
        }

        final int secondsUntilReset = status.getSecondsUntilReset();
        final int callsRemaining = status.getRemaining();
        if (secondsUntilReset < 10 || callsRemaining < 10) {
            final int untilReset = status.getSecondsUntilReset() + 5;
            LOG.info("Rate limit reached. Waiting {} seconds starting at {}...", untilReset, new Date());
            try {
                Thread.sleep(untilReset * 1000);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while sleeping waiting for Twitter.", e);
                Thread.currentThread().interrupt(); // reset interrupt flag
            }
            LOG.info("Resuming...");
        }
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
