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
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import twitter4j.IDs;
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
            OPTIONS.addOption("h", "help", false, "Ask for help with using this tool.");
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
     * Twitter API rate limit data.
     */
    public class LimitData {

        /** The maximum number of calls per limiting window. */
        public int limit;

        /** The remaining number of calls within the current limiting window. */
        public int remaining;

        /**
         * The timestamp, as seconds offset in the modern Unix era,
         * at which the next limiting window will begin.
         */
        public long reset;

        public LimitData(final int limit, final int remaining, final long reset) {
            this.limit = limit;
            this.remaining = remaining;
            this.reset = reset;
        }

        @Override
        public String toString() {
            return "Limit data [remaining:" + this.remaining +
                ",limit:" + this.limit + ",reset:" + this.reset + "]";
        }
    }

    /**
     * Class to package up the response to a call to the Twitter Api.
     */
    public static class ApiCallResponse {

        /** The RateLimitStatus returned by the API call, if successful. */
        public final RateLimitStatus rls;

        /** The records retrieved, of whatever type required (e.g. JsonNodes). */
        public final List<? extends Object> payload;

        /** The error thrown if the API call failed null if it succeeded. */
        public final Throwable error;

        @SuppressWarnings("unchecked")
        public ApiCallResponse(
            final RateLimitStatus status,
            final List<? extends Object> retrieved,
            final Throwable error
        ) {
            this.rls = status;
            this.payload = retrieved;
            this.error = error;
        }

        public boolean succeeded() {
            return error == null;
        }
    }

    private static ApiCallResponse apiCallSuccess(RateLimitStatus rls, List<Object> retrieved) {
        return new ApiCallResponse(rls, retrieved, null);
    }

    private static ApiCallResponse apiCallError(Throwable error, List<Object> retrieved) {
        return new ApiCallResponse(null, retrieved, error);
    }

    public static class IDsApiCallResponse extends ApiCallResponse {
        public final long nextCursor;
        public final long prevCursor;

        public IDsApiCallResponse(
            final RateLimitStatus status,
            final List<Long> retrievedIDs,
            final long nextCursor,
            final long prevCursor,
            final Throwable error
        ) {
            super(status, retrievedIDs, error);
            this.prevCursor = prevCursor;
            this.nextCursor = nextCursor;
        }
    }

    public static void main(String[] args) throws IOException, TwitterException {
        Config cfg = Config.parse(args);
        cfg.check();

        new TwitterUserResourcesRetrieverApp(cfg).run();
    }

    /**
     * For each API endpoint, hold a mutex controlling access to the path.
     * The keys of this map are the endpoint paths.
     */
    public Map<String, ReentrantLock> limitsMutex;

    /**
     * Map from API endpoint to limit data known for that endpoint.
     * Keys of this map are the endpoint paths.
     */
    public Map<String, LimitData> limits;

    /**
     * {@link Config Configuration} as specified via the command line.
     */
    private final Config cfg;

    /**
     * An ObjectMapper for parsing JSON.
     */
    private final ObjectMapper json;

    /**
     * The Twitter API instance, used to communicate with Twitter.
     */
    private Twitter twitter;

    /**
     * A simple callback interface.
     *
     * @param <Arg1> The type of the single argument to the callback.
     * @param <RetType> The return type from this callback's {@link #apply(Object)} method.
     */
    @FunctionalInterface
    interface ApiCall<Arg1, RetType extends ApiCallResponse> {
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
    @SuppressWarnings("unchecked")
    public void run() throws IOException, TwitterException {

        LOG.info("Collecting resources for Twitter ids in: " + this.cfg.identifiersFile);
        LOG.info("* output directory: " + this.cfg.outputDir);
        LOG.info("* tweets:     " + this.cfg.collectTweets);
        LOG.info("* favourites: " + this.cfg.collectFaves);
        LOG.info("* followers:  " + this.cfg.collectFollowers);
        LOG.info("* friends:    " + this.cfg.collectFriends);

        final List<Long> ids = this.loadIDs(this.cfg.identifiersFile);

        LOG.info("Read {} Twitter IDs", ids.size());

        Stopwatch timer = Stopwatch.createStarted();

        if (!Files.exists(Paths.get(this.cfg.outputDir))) {
            LOG.info("Creating output directory {}", this.cfg.outputDir);
            Paths.get(this.cfg.outputDir).toFile().mkdirs();
        }

        this.twitter = this.establishTwitterConnection();

        for (final Long userId : ids) {

            try {
                LOG.info("Looking up {}'s resources", userId);
                //final long idAsLong = Long.parseLong(userId);

                // Retrieve tweets
                if (this.cfg.collectTweets) {
                    List<JsonNode> tweets =
                        (List<JsonNode>) this.makeApiCall(userId, GET_TWEETS, this.createGetTweetsCallback(1)).payload;
                    LOG.info("Retrieved {} tweets", tweets.size());
                }
                // Retrieve favourites
                if (this.cfg.collectFaves) {
                    List<JsonNode> faves =
                        (List<JsonNode>) this.makeApiCall(userId, GET_FAVES, this.createGetFavesCallback(1)).payload;
                    LOG.info("Retrieved {} tweets", faves.size());
                }
                // Retrieve followers
                if (this.cfg.collectFollowers) {
                    LOG.info("Collecting followers of #{} with {}", userId, GET_FOLLOWERS);
                    fetchAndPersistFollowers(userId);
                    LOG.info("Collected followers of #{}", userId);
                }
                // Retrieve friends
                if (this.cfg.collectFriends) {
                    LOG.info("Collecting accounts followed by #{} with {} - NYI", userId, GET_FRIENDS);
                    fetchAndPersistFriends(userId);
                    LOG.info("Collected accounts followed by #{}", userId);
                }

            } catch (InterruptedException e) {
                LOG.warn("Failed to communicate with Twitter somehow", e);
            }
        }
        LOG.info("Retrieval complete in {} seconds.", timer.elapsed(TimeUnit.SECONDS));
    }


    @SuppressWarnings("unchecked")
    private void fetchAndPersistFollowers(final Long userId) throws InterruptedException {
        final String followersFile = outFile(userId + "-followers.txt");
        try (BufferedWriter out = Files.newBufferedWriter(
            Paths.get(followersFile),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        )) {
            long cursor = -1, prevCursor = -1;
            List<Long> followers = Collections.emptyList();
            do {
                final ApiCall<Long, ApiCallResponse> callback = this.createGetFollowersCallback(cursor);
                IDsApiCallResponse resp = (IDsApiCallResponse) this.makeApiCall(userId, GET_FOLLOWERS, callback);
                prevCursor = cursor;
                cursor = resp.nextCursor;
                followers = (List<Long>) resp.payload;

                LOG.info("Collected {} followers, starting at cursor {}", followers.size(), prevCursor);

                for (Long id : followers) {
                    out.append(id.toString() + "\n").flush();
                }
            } while (! followers.isEmpty());

        } catch (IOException e) {
            LOG.warn("Failed to write to {}", followersFile, e);
        }
    }


    @SuppressWarnings("unchecked")
    private void fetchAndPersistFriends(final Long userId) throws InterruptedException {
        final String friendsFile = outFile(userId + "-friends.txt");
        try (BufferedWriter out = Files.newBufferedWriter(
            Paths.get(friendsFile),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        )) {
            long cursor = -1, prevCursor = -1;
            List<Long> friends = Collections.emptyList();
            do {
                final ApiCall<Long, ApiCallResponse> callback = this.createGetFriendsCallback(cursor);
                IDsApiCallResponse resp = (IDsApiCallResponse) this.makeApiCall(userId, GET_FRIENDS, callback);
                prevCursor = cursor;
                cursor = resp.nextCursor;
                friends = (List<Long>) resp.payload;

                LOG.info("Collected {} friends, starting at cursor {}", friends.size(), prevCursor);

                for (Long id : friends) {
                    out.append(id.toString() + "\n").flush();
                }
            } while (! friends.isEmpty());

        } catch (IOException e) {
            LOG.warn("Failed to write to {}", friendsFile, e);
        }
    }


    /**
     * Reads in Twitter config and credentials, establishes a connection to Twitter and then
     * retrieves the current rate limits for the required endpoints.
     *
     * @return A {@link Twitter} instance ready for use.
     * @throws IOException If there is an issue talking to the disk or the network.
     * @throws TwitterException If there is an issue talking with Twitter.
     */
    private Twitter establishTwitterConnection() throws IOException, TwitterException {
        final Configuration config = twitterConfig(this.cfg.credentialsFile, this.cfg.debug);
        final Twitter twitter = new TwitterFactory(config).getInstance();

        this.retrieveRateLimits(twitter);

        return twitter;
    }


    /**
     * Creates a callback to a Twitter API call to fetch the specified page
     * (starting at 1) of tweets for a given ID, in batches of 200.
     *
     * @param pageNo The page of results to request (starting at 1).
     * @return An {@link ApiCallResponse} instance.
     */
    private ApiCall<Long, ApiCallResponse> createGetTweetsCallback(final int pageNo) {
        ApiCall<Long, ApiCallResponse> callback = (Long id) -> {
            List<Object> tweetsRetrieved = Lists.newArrayList();
            try {
                LOG.info("Collecting tweets posted by #{} with {}", id, GET_TWEETS);
                final ResponseList<Status> userTimeline =
                    this.twitter.getUserTimeline(id.longValue(), new Paging(pageNo, 200));
                // extract raw json and write it to file
                final String rawJsonTweets = TwitterObjectFactory.getRawJSON(userTimeline);
                final StringBuilder tweetsToSave = new StringBuilder();
                for (JsonNode tweetNode : this.json.readTree(rawJsonTweets)) {
                    tweetsRetrieved.add(tweetNode);
                    tweetsToSave.append(tweetNode.toString()).append('\n');
                }
                String tweetsFile = this.outFile(id.toString() + "-tweets.json");
                saveText(tweetsToSave.toString(), tweetsFile);
                LOG.info("Wrote {} tweets to {}", tweetsRetrieved.size(), tweetsFile);
                return apiCallSuccess(userTimeline.getRateLimitStatus(), tweetsRetrieved);
            } catch (IOException | TwitterException e) {
                LOG.warn("Barfed parsing JSON or saving to file tweets for {}.", id, e);
                return apiCallError(e, tweetsRetrieved);
            }
        };
        return callback;
    }


    /**
     * Creates a callback to a Twitter API call to fetch the specified page
     * (starting at 1) of favourite tweets for a given ID, in batches of 200.
     *
     * @param pageNo The page of results to request (starting at 1).
     * @return An {@link ApiCallResponse} instance.
     */
    private ApiCall<Long, ApiCallResponse> createGetFavesCallback(final int pageNo) {
        ApiCall<Long, ApiCallResponse> callback = (Long id) -> {
            List<Object> favesRetrieved = Lists.newArrayList();
            try {
                LOG.info("Collecting tweets favourited by #{} with {}", id, GET_FAVES);
                final ResponseList<Status> favesTimeline =
                    this.twitter.getFavorites(id.longValue(), new Paging(pageNo, 200));
                // extract raw json and write it to file
                final String rawJsonTweets = TwitterObjectFactory.getRawJSON(favesTimeline);
                final StringBuilder favesToSave = new StringBuilder();
                for (JsonNode tweetNode : this.json.readTree(rawJsonTweets)) {
                    favesRetrieved.add(tweetNode);
                    favesToSave.append(tweetNode.toString()).append('\n');
                }
                String favesFile = this.outFile(id.toString() + "-favourites.json");
                saveText(favesToSave.toString(), favesFile);
                LOG.info("Wrote {} favourites to {}", favesRetrieved.size(), favesFile);
                return apiCallSuccess(favesTimeline.getRateLimitStatus(), favesRetrieved);
            } catch (IOException | TwitterException e) {
                LOG.warn("Barfed parsing JSON or saving to file favourites for {}.", id, e);
                return apiCallError(e, favesRetrieved);
            }
        };
        return callback;
    }


    /**
     * Creates a callback to a Twitter API call to fetch a batch of followers
     * (starting at <code>cursor</code>, which should be -1 to begin) for a given ID.
     *
     * @param cursor The cursor to start the batch of followers to request (use -1 to start).
     * @return An {@link ApiCallResponse} instance.
     */
    private ApiCall<Long, ApiCallResponse> createGetFollowersCallback(final long cursor) {
        ApiCall<Long, ApiCallResponse> callback = (Long id) -> {
            List<Long> batchOfIDs = Lists.newArrayList();
            try {
                LOG.info("Collecting followers of account {} with {}", id, GET_FOLLOWERS);
                final IDs response = this.twitter.getFollowersIDs(id.longValue(), cursor);
                for (long l : response.getIDs()) {
                    batchOfIDs.add(Long.valueOf(l));
                }
                return new IDsApiCallResponse(
                    response.getRateLimitStatus(),
                    batchOfIDs,
                    response.getNextCursor(),
                    response.getPreviousCursor(),
                    null);
            } catch (TwitterException e) {
                LOG.warn("Barfed parsing JSON or saving to file followers for {}.", id, e);
                return new IDsApiCallResponse(null, batchOfIDs, cursor, cursor, e);
            }
        };
        return callback;
    }


    /**
     * Creates a callback to a Twitter API call to fetch a batch of friends (followees)
     * (starting at <code>cursor</code>, which should be -1 to begin) for a given ID.
     *
     * @param cursor The cursor to start the batch of followers to request (use -1 to start).
     * @return An {@link ApiCallResponse} instance.
     */
    private ApiCall<Long, ApiCallResponse> createGetFriendsCallback(final long cursor) {
        ApiCall<Long, ApiCallResponse> callback = (Long id) -> {
            List<Long> batchOfIDs = Lists.newArrayList();
            try {
                LOG.info("Collecting friends of account {} with {}", id, GET_FRIENDS);
                final IDs response = this.twitter.getFriendsIDs(id.longValue(), cursor);
                for (long l : response.getIDs()) {
                    batchOfIDs.add(Long.valueOf(l));
                }
                return new IDsApiCallResponse(
                    response.getRateLimitStatus(),
                    batchOfIDs,
                    response.getNextCursor(),
                    response.getPreviousCursor(),
                    null);
            } catch (TwitterException e) {
                LOG.warn("Barfed parsing JSON or saving to file friends of {}.", id, e);
                return new IDsApiCallResponse(null, batchOfIDs, cursor, cursor, e);
            }
        };
        return callback;
    }


    /**
     * Invokes the Twitter <code>endpoint</code> with the given
     * <code>twitterId</code> and <code>callback</code> taking into
     * account Twitter rate limits.
     *
     * @param twitterId The Twitter ID argument for the endpoint.
     * @param endpoint The Twitter RESTful endpoint being invoked.
     * @param callback The callback to execute when the endpoint invocation returns.
     * @return Records retrieved by the API call.
     * @throws InterruptedException If we're interrupted while awaiting a response.
     */
    private ApiCallResponse makeApiCall(
        final long twitterId,
        final String endpoint,
        final ApiCall<Long, ApiCallResponse> callback
    ) throws InterruptedException {
        this.debug("Acquiring mutex for endpoint {}.", endpoint);
        this.limitsMutex.get(endpoint).lockInterruptibly();
        try {
            this.debug("Acquired mutex for endpoint {}.", endpoint);
            LimitData limitData = this.limits.get(endpoint);
            this.debug("There are {} remaining calls to {}.", limitData.remaining, endpoint);
            if (limitData.remaining == 0) {
                // Fudge added to the advertised reset time to guard against local sleep duration
                // jitter and clock skew between local clock and Twitter's clock.
                long sleepDurationMillis =
                    limitData.reset * 1000 - System.currentTimeMillis() + SLEEP_DURATION_FUDGE_AMOUNT;
                if (sleepDurationMillis > 0) {
                    LOG.info(
                        "Sleeping {} seconds to get past the reset timestamp {} for endpoint {}.",
                        sleepDurationMillis / 1000f,
                        this.formatNicely(limitData.reset),
                        endpoint
                    );
                    Thread.sleep(sleepDurationMillis);
                    LOG.info("Wait for {} over. Starting again.", endpoint);
                } else {
                    this.debug(
                        "However, the limit for {} should have reset by now (timestamp is {}).",
                        endpoint, this.formatNicely(limitData.reset)
                    );
                }
            }

            ApiCallResponse resp = callback.apply(twitterId);

            if (resp.succeeded()) {
                RateLimitStatus rls = resp.rls;
                limitData.remaining = rls.getRemaining();
                this.debug("Endpoint {} has {} calls remaining.", endpoint, limitData.remaining);
                limitData.reset = rls.getResetTimeInSeconds();
                this.debug("Endpoint {} has a new reset time: {}.", endpoint, limitData.reset);
            }

            return resp;

        } finally {
            this.limitsMutex.get(endpoint).unlock();
            this.debug("Released mutex for limit key {}", endpoint);
        }
    }

    private void debug(String format, Object... values) {
        if (this.cfg.debug) LOG.info(format, values);
    }

    private String outFile(String filename) {
        return this.cfg.outputDir + FILE_SEPARATOR + filename;
    }


    /**
     * Formats an epoch second nicely as a human-readable date/timestamp.
     * @param epochSecond The epoch second to render readable.
     * @return A human-readable date/timestamp.
     */
    private String formatNicely(long epochSecond) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
            ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneId.systemDefault())
        );
    }


    /**
     * Retrieve the current Twitter rate limits and populate the
     * {@link #limits} and {@link #limitsMutex} maps.
     *
     * @param twitter The connection to Twitter.
     * @throws TwitterException If an error occurs talking with Twitter.
     */
    private void retrieveRateLimits(Twitter twitter) throws TwitterException {
        this.limitsMutex = Maps.newConcurrentMap();
        this.limits = Maps.newConcurrentMap();
        Map<String, RateLimitStatus> rateLimitStatuses = twitter.getRateLimitStatus(
            "statuses",  // endpoint family for "/statuses/user_timeline",
            "favorites", // endpoint family for "/favorites/list",
            "followers", // endpoint family for "/followers/ids",
            "friends"    // endpoint family for "/friends/ids"
        );
        rateLimitStatuses.forEach((endpoint, rls) -> {
            this.limitsMutex.put(endpoint, new ReentrantLock(true));
            this.limits.put(endpoint, this.limitDataFrom(rls));
            this.debug("Endpoint: {} -> {}", endpoint, this.limits.get(endpoint));
        });
    }


    /**
     * Creates a {@link LimitData} from a {@link RateLimitStatus}.
     *
     * @param rls The RateLimitStatus to convert.
     * @return A LimitData corresponding to the given RateLimitStatus.
     */
    private LimitData limitDataFrom(RateLimitStatus rls) {
        return new LimitData(rls.getLimit(), rls.getRemaining(), rls.getResetTimeInSeconds());
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


    /**
     * Reads Twitter IDs (expected to be longs) from the given
     * <code>idsFile</code> into a list. IDs are listed in the file
     * as one ID per line. Lines starting with '#' will be ignored.
     * Content on a line following a '#' will be ignored. Example file:
     * <pre>
     * # Twitter ID file
     * 123 # @pippin
     * 456 # @merry
     * 789 # @sam
     * 101 # @frodo
     * </pre>
     *
     * @param idsFile Path to a file with Twitter IDs (long values)
     * @return A list of Twitter IDs.
     * @throws IOException If there's an issue reading the file or parsing the IDs.
     */
    private List<Long> loadIDs(final String idsFile) throws IOException {
        return Files.readAllLines(Paths.get(idsFile)).stream()
            .map(l -> l.split("#")[0].trim())
            .filter(l -> l.length() > 0 && ! l.startsWith("#"))
            .map(Long::parseLong)
            .collect(Collectors.toList());
    }

    /**
     * Builds the {@link Configuration} object with which to connect to Twitter,
     * including credentials and looks for proxy information in
     * <code>proxy.properties</code> if it's present.
     *
     * @param credentialsFile Property file of Twitter credentials.
     * @param debug Debug flag to pass to Twitter4j's config builder.
     * @return a Twitter4j {@link Configuration} object.
     * @throws IOException if there's an error loading the application's
     *         {@link #credentialsFile}.
     */
    private static Configuration twitterConfig
        (final String credentialsFile, final boolean debug) throws IOException {
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
     * is not supplied, asks for it in the console. The properties collected are
     * also shunted into the System properties object.
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
