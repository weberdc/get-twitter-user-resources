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
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import twitter4j.*;
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
    private static final long DEFAULT_MAX_ID = Long.MAX_VALUE;

    /**
     * This fudge amount (milliseconds) is added to the time a thread goes to
     * sleep when it needs to wait until the next rate limiting window. This is
     * to guard against clock skew between the local clock and Twitter's clock,
     * and also the inherent jitter in Thread.sleep(). 10 seconds seems like a
     * reasonable amount.
     */
    private static final int SLEEP_DURATION_FUDGE_AMOUNT = 10000;

    /** Batches will miss deleted tweets and IDs, so account for that when looking at batch sizes. */
    private static final int BATCH_FUDGE = 100;

    /**
     * The maximum number of statuses (tweets) to request when asking for timeline
     * tweets or favourites from Twitter (as specified by Twitter's API docs).
     */
    private static final int TWEET_BATCH_SIZE = 200;

    /**
     * The maximum number of profiles to request when asking Twitter's API.
     */
    private static final int PROFILE_BATCH_SIZE = 100;

    /**
     * The maximum number of IDs to request when asking for friend or follower
     * IDs from Twitter (as specified by Twitter's API docs).
     *
     * @see <a href="https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-followers-ids">GET /followers/ids - Twitter Developers</a>
     */
    private static final int ID_BATCH_SIZE = /*200*/ 5000;

    private static final String GET_TWEETS = "/statuses/home_timeline";
    private static final String GET_FAVES = "/favorites/list";
    private static final String GET_FOLLOWERS = "/followers/ids";
    private static final String GET_FRIENDS = "/friends/ids";
    private static final String GET_MENTIONS = "/search/tweets";

    private static final String DEFAULT_CREDENTIALS_PATH = "./twitter.properties";
    private static final String FILE_SEPARATOR = System.getProperty("file.separator", "/");
    private static final Logger LOG = LoggerFactory.getLogger(TwitterUserResourcesRetrieverApp.class);
    private static final int MENTION_BATCH_SIZE = 100;

    private static Properties credentials;

    /** Class for managing command line options to this application. */
    static class Config {
        private static final Options OPTIONS = new Options();
        static {
            OPTIONS.addOption("i", "identifiers-file", true, "File of Twitter screen names");
            OPTIONS.addOption(longOpt("ids", "Inline, comma-delimited listing of Twitter IDs to look up (alternative to --identifiers-file)").hasArg().create());
            OPTIONS.addOption(longOpt("tweets", "Collect statuses (tweets)").create());
            OPTIONS.addOption(longOpt("target-tweet-count", "Collect at least this many statuses (tweets) (default: 200)").hasArg().create());
            OPTIONS.addOption(longOpt("favourites", "Collect favourited statuses").create());
            OPTIONS.addOption(longOpt("target-favourite-count", "Collect at least this many favourited statuses (default: 200)").hasArg().create());
            OPTIONS.addOption(longOpt("followers", "Collect follower IDs").create());
            OPTIONS.addOption(longOpt("target-followers-count", "Collect at least this many follower IDs (default: 4900)").hasArg().create());
            OPTIONS.addOption(longOpt("friends", "Collect friend (followee) IDs").create());
            OPTIONS.addOption(longOpt("target-friends-count", "Collect at least this many friend (followee) IDs (default: 4900)").hasArg().create());
            OPTIONS.addOption(longOpt("mentions", "Collect mentions of this account").create());
            OPTIONS.addOption(longOpt("target-mention-count", "Collect at least this many mentions of this account (default: 100)").hasArg().create());
            OPTIONS.addOption("o", "output-directory", true, "Directory to which to write profiles (default: ./output)");
            OPTIONS.addOption("c", "credentials", true, "File of Twitter credentials (default: ./twitter.properties)");
            OPTIONS.addOption("d", "debug", false, "Turn on debugging information (default: false)");
            OPTIONS.addOption("h", "help", false, "Ask for help with using this tool.");
        }

        private static OptionBuilder longOpt(String name, String description) {
            return OptionBuilder.withLongOpt(name).withDescription(description);
        }

        /** Prints how the app ought to be used and causes the VM to exit. */
        private static void printUsageAndExit() {
            new HelpFormatter().printHelp("TwitterUserResourcesRetrieverApp", OPTIONS);
            System.exit(0);
        }

        private String idsFile = null;
        private boolean fetchTweets = false;
        private int numTweetsToFetch = 200;
        private boolean fetchFaves = false;
        private int numFavouritesToFetch = 200;
        private boolean fetchMentions = false;
        private int numMentionsToFetch = MENTION_BATCH_SIZE;
        private boolean fetchFollowers = false;
        private int numFollowersToFetch = 4900;
        private boolean fetchFriends = false;
        private int numFriendsToFetch = 4900;
        private String outputDir = "./output";
        private String credentialsFile = "./twitter.properties";
        private boolean debug = false;
        private String[] ids;

        static Config parse(String[] args) {
            final CommandLineParser parser = new BasicParser();
            Config cfg = new Config();
            try {
                final CommandLine cmd = parser.parse(OPTIONS, args);
                if (cmd.hasOption('i')) cfg.idsFile = cmd.getOptionValue('i');
                if (cmd.hasOption("ids")) cfg.ids = cmd.getOptionValue("ids").split(",");
                if (cmd.hasOption("tweets")) cfg.fetchTweets = true;
                if (cmd.hasOption("target-tweet-count")) { // 200 by default
                    cfg.numTweetsToFetch = Integer.parseInt(cmd.getOptionValue("target-tweet-count"));
                }
                if (cmd.hasOption("favourites")) cfg.fetchFaves = true;
                if (cmd.hasOption("target-favourite-count")) { // 200 by default
                    cfg.numFavouritesToFetch = Integer.parseInt(cmd.getOptionValue("target-favourite-count"));
                }
                if (cmd.hasOption("followers")) cfg.fetchFollowers = true;
                if (cmd.hasOption("target-followers-count")) { // 100 by default
                    cfg.numFollowersToFetch = Integer.parseInt(cmd.getOptionValue("target-followers-count"));
                }
                if (cmd.hasOption("friends")) cfg.fetchFriends = true;
                if (cmd.hasOption("target-friends-count")) { // 100 by default
                    cfg.numFriendsToFetch = Integer.parseInt(cmd.getOptionValue("target-friends-count"));
                }
                if (cmd.hasOption("mentions")) cfg.fetchMentions = true;
                if (cmd.hasOption("target-mention-count")) { // 100 by default
                    cfg.numMentionsToFetch = Integer.parseInt(cmd.getOptionValue("target-mention-count"));
                }
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
            if (this.idsFile == null && (this.ids == null || this.ids.length == 0)) {
                printUsageAndExit();
            }
        }
    }

    /** Twitter API rate limit data. */
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
            return String.format(
                "Limit data [remaining:%d,limit:%d,reset:%d",
                this.remaining, this.limit, this.reset
            );
        }
    }

    /** Class to package up the response to a call to the Twitter API. */
    public static class ApiCallResponse {

        /** The RateLimitStatus returned by the API call, if successful. */
        public final RateLimitStatus rls;

        /** The records retrieved, of whatever type required (e.g. JsonNodes). */
        public final List<? extends Object> payload;

        /** The error thrown if the API call failed null if it succeeded. */
        public final Throwable error;

        public ApiCallResponse(
            final RateLimitStatus status,
            final List<? extends Object> retrieved,
            final Throwable error
        ) {
            this.rls = status;
            this.payload = retrieved;
            this.error = error;
        }

        public boolean hasARateLimitStatusUpdate() {
            return this.rls != null;
        }
    }

    private static ApiCallResponse apiCallSuccess(RateLimitStatus rls, List<Object> retrieved) {
        return new ApiCallResponse(rls, retrieved, null);
    }

    private static ApiCallResponse apiCallError(Throwable error, List<Object> retrieved) {
        return new ApiCallResponse(null, retrieved, error);
    }

    private static ApiCallResponse apiCallError(RateLimitStatus rls, List<Object> retrieved, Throwable error) {
        return new ApiCallResponse(rls, retrieved, error);
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
     * Holds retrieved Twitter profiles, indexed by their IDs.
     */
    private final Map<Long, User> profileMap = Maps.newTreeMap();


    /**
     * A simple callback interface.
     *
     * @param <Arg1> The type of the single argument to the callback.
     * @param <RetType> The return type from this callback's {@link #apply(Object)} method.
     */
    @FunctionalInterface
    interface ApiCall<Arg1, RetType extends ApiCallResponse> {
        RetType apply(Arg1 arg1);
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

        LOG.info("Collecting resources for Twitter ids in: " + this.cfg.idsFile);
        LOG.info("* output directory: " + this.cfg.outputDir);
        LOG.info("* tweets:     " + this.cfg.fetchTweets + (this.cfg.fetchTweets ? " [" + this.cfg.numTweetsToFetch + "]" : ""));
        LOG.info("* favourites: " + this.cfg.fetchFaves + (this.cfg.fetchFaves ? " [" + this.cfg.numFavouritesToFetch + "]" : ""));
        LOG.info("* mentions:   " + this.cfg.fetchMentions + (this.cfg.fetchMentions ? " [" + this.cfg.numMentionsToFetch + "]" : ""));
        LOG.info("* followers:  " + this.cfg.fetchFollowers + (this.cfg.fetchFollowers ? " [" + this.cfg.numFollowersToFetch + "]" : ""));
        LOG.info("* friends:    " + this.cfg.fetchFriends + (this.cfg.fetchFriends ? " [" + this.cfg.numFriendsToFetch + "]" : ""));

        final Set<Long> ids = this.loadIDs(this.cfg);

        LOG.info("Read {} Twitter IDs", ids.size());

        Stopwatch timer = Stopwatch.createStarted();

        if (!Files.exists(Paths.get(this.cfg.outputDir))) {
            LOG.info("Creating output directory {}", this.cfg.outputDir);
            Paths.get(this.cfg.outputDir).toFile().mkdirs();
        }

        this.twitter = this.establishTwitterConnection();

        // retrieve profiles, first, to get screen names
        for (final List<Long> batch : Lists.partition(Lists.newArrayList(ids), PROFILE_BATCH_SIZE)) {
            try {
                LOG.info("Looking up {} users' profiles", batch.size());

                // ask Twitter for profiles
                final long[] allocation = new long[batch.size()];
                for (int i = 0; i < batch.size(); i++) {
                    allocation[i] = batch.get(i);
                }
                final ResponseList<User> profiles = twitter.lookupUsers(allocation);

                final String rawJSON = TwitterObjectFactory.getRawJSON(profiles);

                profiles.forEach(p -> profileMap.put(p.getId(), p));

                for (JsonNode profileNode : json.readTree(rawJSON)) {
                    final String profileFile = outFile(profileNode.get("id_str").asText() + "-profile.json");
                    try (BufferedWriter out = createFreshWriter(profileFile, true)) {
                        out.append(profileNode.toString()).append('\n').flush();
                    } catch (IOException e) {
                        LOG.warn(
                            "Couldn't save profile @" + profileNode.get("screen_name").asText() +
                            " (#" + profileNode.get("id_str").asText() + ")",
                            e
                        );
                    }
                }

                pauseIfNecessary(profiles.getRateLimitStatus());
            } catch (TwitterException e) {
                LOG.warn("Failed to communicate with Twitter somehow while getting profiles", e);
            }

        }

        int runningTotal = 0;
        for (final Long userId : ids) {

            try {
                LOG.info("Looking up {}'s resources", userId);

                // Retrieve tweets
                if (this.cfg.fetchTweets) {
                    LOG.info("Collecting tweets by #{} with {}", userId, GET_FOLLOWERS);

                    fetchAndPersistTweets(
                        userId, "tweets", GET_TWEETS, cfg.numTweetsToFetch, this::createGetTweetsCallback
                    );

                    LOG.info("Collected tweets of #{}", userId);
                }
                // Retrieve favourites
                if (this.cfg.fetchFaves) {
                    LOG.info("Collecting favourites made by #{} with {}", userId, GET_FOLLOWERS);

                    fetchAndPersistTweets(
                        userId, "favourites", GET_FAVES, cfg.numFavouritesToFetch, this::createGetFavesCallback
                    );

                    LOG.info("Collected favourites of #{}", userId);
                }
                // Retrieve mentions
                if (this.cfg.fetchMentions) {
                    LOG.info("Collecting mentions of #{} with {}", userId, GET_MENTIONS);

                    fetchAndPersistTweets(
                        userId, "mentions", GET_MENTIONS, cfg.numMentionsToFetch, this::createGetMentionsCallback
                    );

                    LOG.info("Collected mentions of #{}", userId);
                }
                // Retrieve followers
                if (this.cfg.fetchFollowers) {
                    LOG.info("Collecting followers of #{} with {}", userId, GET_FOLLOWERS);

                    fetchAndPersistIDs(userId, "followers", cfg.numFollowersToFetch, GET_FOLLOWERS, this::createGetFollowersCallback);

                    LOG.info("Collected followers of #{}", userId);
                }
                // Retrieve friends
                if (this.cfg.fetchFriends) {
                    LOG.info("Collecting accounts followed by #{} with {}", userId, GET_FRIENDS);

                    fetchAndPersistIDs(userId, "friends", cfg.numFriendsToFetch, GET_FRIENDS, this::createGetFriendsCallback);

                    LOG.info("Collected accounts followed by #{}", userId);
                }

                LOG.info("Collected resources for {} of {} accounts so far...", ++runningTotal, ids.size());

            } catch (InterruptedException e) {
                LOG.warn("Failed to communicate with Twitter somehow", e);
            }
        }
        LOG.info("Retrieval complete in {} seconds.", timer.elapsed(TimeUnit.SECONDS));
    }

    /**
     * If the provided {@link RateLimitStatus} indicates that we are about to break the rate
     * limit, in terms of number of calls or time window, then sleep for the rest of the period.
     *
     * @param status The current status of the our calls to Twitter
     */
    private void pauseIfNecessary(final RateLimitStatus status) {
        if (status == null) {
            return;
        }

        final int secondsUntilReset = status.getSecondsUntilReset();
        final int callsRemaining = status.getRemaining();
        if (secondsUntilReset < 10 || callsRemaining < 10) {
            final int untilReset = Math.abs(status.getSecondsUntilReset()) + 10;
            LOG.info("Rate limit reached. Waiting {} seconds starting at {}...", untilReset, new Date());
            try {
                Thread.sleep(untilReset * 1000);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while sleeping waiting for Twitter.", e);
                Thread.currentThread().interrupt(); // reset interrupt flag
            }
            LOG.info("Resuming... at {}", new Date());
        }
    }



    /**
     * Fetches at least {@code fetchLimit} {@code tweetType} statuses (tweets)
     * connected with {@code userId} using a callback created by the given
     * {@code callbackGenerator} and the given {@code endpoint}.
     *
     * @param userId The ID of the Twitter user of interest.
     * @param tweetType The type of tweet to collect (e.g. "tweets", "favourites").
     * @param endpoint The Twitter RESTful endpoint that will be used.
     * @param fetchLimit The minimum number of statuses to fetch, if possible (there may not be that many).
     * @param callbackGenerator A function to generate an API-call specific callback to fetch the statuses.
     * @throws InterruptedException If something fails while sleeping to adhere to Twitter's rate limits.
     */
    @SuppressWarnings("unchecked")
    private void fetchAndPersistTweets(
        final Long userId,
        final String tweetType,
        final String endpoint,
        final int fetchLimit,
        final Function<Long, ApiCall<Long, ApiCallResponse>> callbackGenerator
    ) throws InterruptedException {

        final String tweetsFile = outFile(userId + "-" + tweetType + ".json");
        try (BufferedWriter out = createFreshWriter(tweetsFile, true)) {
            List<JsonNode> tweetNodes = Collections.emptyList();
            int tweetsFetched = 0;
            long maxId = DEFAULT_MAX_ID; // i.e. get tweets younger than this value
            boolean bail = false;
            do {
                final ApiCall<Long, ApiCallResponse> callback = callbackGenerator.apply(maxId);
                tweetNodes = (List<JsonNode>) this.makeApiCall(userId, endpoint, callback).payload;
                tweetsFetched += tweetNodes.size();
                LOG.info("Retrieved {} of {} {}", tweetsFetched, fetchLimit, tweetType);

                long minId = DEFAULT_MAX_ID;
                for (JsonNode tweetNode : tweetNodes) {
                    long tweetId = tweetNode.get("id").asLong();
                    minId = Math.min(minId, tweetId - 1);
                    out.append(tweetNode.toString()).append('\n').flush();
                }
                maxId = minId;
                LOG.info("Wrote another {} " + tweetType + " to {}", tweetNodes.size(), tweetsFile);

                bail = (tweetNodes.size() < TWEET_BATCH_SIZE - BATCH_FUDGE); // not enough to fill a batch
            } while (! bail && ! tweetNodes.isEmpty() && tweetsFetched < fetchLimit);

        } catch (IOException e) {
            LOG.warn("Failed to write to {}", tweetsFile, e);
        }
    }


    /**
     * Fetches all {@code idType} (e.g. "friends", "followers") IDs
     * connected with {@code userId} using a callback created by the
     * given {@code callbackGenerator} and the given {@code endpoint}.
     *
     * @param userId The ID of the Twitter user of interest.
     * @param idType The type of IDs to collect (e.g. "friends", "followers").
     * @param fetchTarget An upper bound on the number of IDs to fetch.
     * @param endpoint The Twitter RESTful endpoint that will be used.
     * @param callbackGenerator A function to generate an API-call specific callback to fetch the statuses.
     * @throws InterruptedException If something fails while sleeping to adhere to Twitter's rate limits.
     */
    @SuppressWarnings("unchecked")
    private void fetchAndPersistIDs(
        final Long userId,
        final String idType,
        final int fetchTarget,
        final String endpoint,
        final Function<Long, ApiCall<Long, ApiCallResponse>> callbackGenerator
    ) throws InterruptedException {
        final String idsFile = outFile(userId + "-" + idType + ".txt");
        int idsFetched = 0;
        try (BufferedWriter out = createFreshWriter(idsFile, true)) {
            long cursor = -1, prevCursor;
            List<Long> ids = Collections.emptyList();
            boolean bail = false;
            do {
                final ApiCall<Long, ApiCallResponse> callback = callbackGenerator.apply(cursor);
                IDsApiCallResponse resp = (IDsApiCallResponse) this.makeApiCall(userId, endpoint, callback);
                prevCursor = cursor;
                cursor = resp.nextCursor;
                ids = (List<Long>) resp.payload;
                idsFetched += ids.size();

                LOG.info("Collected {} of {} {}, starting at cursor {}", idsFetched, fetchTarget, idType, prevCursor);

                for (Long id : ids) {
                    out.append(id.toString() + "\n").flush();
                }

                bail = (
                    (ids.size() < ID_BATCH_SIZE - BATCH_FUDGE) || // not enough to fill a batch
                    idsFetched >= fetchTarget // we have enough overall
                );

            } while (! bail && ! ids.isEmpty());

        } catch (IOException e) {
            LOG.warn("Failed to write to {}", idsFile, e);
        }
    }


    /**
     * Creates a new {@link BufferedWriter} to a file, creating it if it's not there,
     * making it writable (<em>NB</em> truncating it before writing to it if
     * {@code truncateFirst} is true).
     *
     * @param filename The path of the file to which to write.
     * @param truncateFirst Truncate the file if it exists, before writing to it.
     * @return A {@link BufferedWriter} for writing to the given file.
     * @throws IOException If an error occurs attempting to establish the writer.
     */
    private BufferedWriter createFreshWriter(final String filename, boolean truncateFirst) throws IOException {
        if (truncateFirst) {
            return Files.newBufferedWriter(
                Paths.get(filename),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        } else {
            return Files.newBufferedWriter(
                Paths.get(filename),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
            );
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
     * Create a {@link Paging} instance for collecting batches of tweets older than
     * {@code maxId}. if {@code maxId} is {@link #DEFAULT_MAX_ID}, then a {@link Paging}
     * instance for the first page of results is returned.
     *
     * @param maxId The maximum ID of tweets being requested (i.e. get tweets older than this).
     * @return A suitably configured {@link Paging} instance.
     */
    private Paging tweetsBefore(final long maxId) {
        if (maxId == DEFAULT_MAX_ID) {
            return new Paging(1, TWEET_BATCH_SIZE);
        } else {
            Paging pagingInfo = new Paging();
            pagingInfo.setCount(TWEET_BATCH_SIZE);
            pagingInfo.setMaxId(maxId);
            return pagingInfo;
        }
    }


    /**
     * Creates a callback to a Twitter API call to fetch the tweets younger
     * (posted earlier) than {@code maxId} for a given Twitter ID (the argument
     * to the callback), in batches of {@link #TWEET_BATCH_SIZE}.
     *
     * @param maxId Get tweets with IDs younger (less) than this ID.
     * @return An {@link ApiCallResponse} instance.
     */
    private ApiCall<Long, ApiCallResponse> createGetTweetsCallback(final long maxId) {
        return (Long id) -> {
            List<Object> tweetsRetrieved = Lists.newArrayList();
            try {
                LOG.info("Collecting tweets posted by #{} with {}", id, GET_TWEETS);
                final ResponseList<Status> userTimeline = this.twitter.getUserTimeline(id.longValue(), tweetsBefore(maxId));
                // extract raw json and create a payload from it
                final String rawJsonTweets = TwitterObjectFactory.getRawJSON(userTimeline);
                for (JsonNode tweetNode : this.json.readTree(rawJsonTweets)) {
                    tweetsRetrieved.add(tweetNode);
                }
                return apiCallSuccess(userTimeline.getRateLimitStatus(), tweetsRetrieved);
            } catch (TwitterException e) {
                logTwitterApiException(id, e);
                return apiCallError(e.getRateLimitStatus(), tweetsRetrieved, e);
            } catch (IOException e) {
                LOG.warn("Barfed parsing JSON or saving to file tweets for {}.", id, e);
                return apiCallError(e, tweetsRetrieved);
            }
        };
    }


    /**
     * Creates a callback to a Twitter API call to fetch the favourited tweets younger
     * (posted earlier) than {@code maxId} for a given Twitter ID (the argument
     * to the callback), in batches of {@link #TWEET_BATCH_SIZE}.
     *
     * @param maxId Get tweets with IDs younger (less) than this ID.
     * @return An {@link ApiCallResponse} instance.
     */
    private ApiCall<Long, ApiCallResponse> createGetFavesCallback(final long maxId) {
        return (Long id) -> {
            List<Object> favesRetrieved = Lists.newArrayList();
            try {
                LOG.info("Collecting tweets favourited by #{} with {}", id, GET_FAVES);
                final ResponseList<Status> favesTimeline = this.twitter.getFavorites(id.longValue(), tweetsBefore(maxId));
                // extract raw json and collect it for the response
                final String rawJsonTweets = TwitterObjectFactory.getRawJSON(favesTimeline);
                for (JsonNode tweetNode : this.json.readTree(rawJsonTweets)) {
                    favesRetrieved.add(tweetNode);
                }
                return apiCallSuccess(favesTimeline.getRateLimitStatus(), favesRetrieved);
            } catch (TwitterException e) {
                logTwitterApiException(id, e);
                return apiCallError(e.getRateLimitStatus(), favesRetrieved, e);
            } catch (IOException e) {
                LOG.warn("Barfed parsing JSON or saving to file tweets for {}.", id, e);
                return apiCallError(e, favesRetrieved);
            }
        };
    }


    /**
     * Creates a callback to a Twitter API call to search for mentions younger
     * (posted earlier) than {@code maxId} for a given Twitter ID (the argument
     * to the callback), in batches of {@link #TWEET_BATCH_SIZE}.
     *
     * @param maxId Get tweets with IDs younger (less) than this ID.
     * @return An {@link ApiCallResponse} instance.
     */
    private ApiCall<Long, ApiCallResponse> createGetMentionsCallback(final long maxId) {
        return (Long id) -> {
            final List<Object> mentionsRetrieved = Lists.newArrayList();
            try {
                final String screenName = "@" + profileMap.get(id).getScreenName();

                LOG.info("Collecting tweets mentioning {} (#{}) with {}", screenName, id, GET_MENTIONS);

                final Query mentionsQuery = new Query(screenName).count(MENTION_BATCH_SIZE);
                final QueryResult mentions = this.twitter.search(mentionsQuery);
                // extract raw json and collect it for the response
                for (Status mention : mentions.getTweets()) {
                    final String rawMention = TwitterObjectFactory.getRawJSON(mention);
                    mentionsRetrieved.add(this.json.readTree(rawMention));
                }
                return apiCallSuccess(mentions.getRateLimitStatus(), mentionsRetrieved);
            } catch (TwitterException e) {
                logTwitterApiException(id, e);
                return apiCallError(e.getRateLimitStatus(), mentionsRetrieved, e);
            } catch (IOException e) {
                LOG.warn("Barfed parsing JSON or saving to file tweets for {}.", id, e);
                return apiCallError(e, mentionsRetrieved);
            }
        };
    }


    /**
     * Creates a callback to a Twitter API call to fetch a batch of followers
     * (starting at <code>cursor</code>, which should be -1 to begin) for a given ID.
     *
     * @param cursor The cursor to start the batch of followers to request (use -1 to start).
     * @return An {@link ApiCallResponse} instance.
     */
    private ApiCall<Long, ApiCallResponse> createGetFollowersCallback(final long cursor) {
        return (Long id) -> {
            List<Long> batchOfIDs = Lists.newArrayList();
            try {
                LOG.info("Collecting followers of account #{} with {}", id, GET_FOLLOWERS);
                final IDs response = this.twitter.getFollowersIDs(id.longValue(), cursor);
                Arrays.stream(response.getIDs())
                //    .map(Long::valueOf)
                    .forEach(batchOfIDs::add);
                return new IDsApiCallResponse(
                    response.getRateLimitStatus(),
                    batchOfIDs,
                    response.getNextCursor(),
                    response.getPreviousCursor(),
                    null
                );
            } catch (TwitterException e) {
                logTwitterApiException(id, e);
                return new IDsApiCallResponse(e.getRateLimitStatus(), batchOfIDs, cursor, cursor, e);
            }
        };
    }


    /**
     * Creates a callback to a Twitter API call to fetch a batch of friends (followees)
     * (starting at <code>cursor</code>, which should be -1 to begin) for a given ID.
     *
     * @param cursor The cursor to start the batch of followers to request (use -1 to start).
     * @return An {@link ApiCallResponse} instance.
     */
    private ApiCall<Long, ApiCallResponse> createGetFriendsCallback(final long cursor) {
        return (Long id) -> {
            List<Long> batchOfIDs = Lists.newArrayList();
            try {
                LOG.info("Collecting friends of account #{} with {}", id, GET_FRIENDS);
                final IDs response = this.twitter.getFriendsIDs(id.longValue(), cursor);
                Arrays.stream(response.getIDs())
                 //   .map(Long::valueOf)
                    .forEach(batchOfIDs::add);
                return new IDsApiCallResponse(
                    response.getRateLimitStatus(),
                    batchOfIDs,
                    response.getNextCursor(),
                    response.getPreviousCursor(),
                    null
                );
            } catch (TwitterException e) {
                logTwitterApiException(id, e);
                return new IDsApiCallResponse(e.getRateLimitStatus(), batchOfIDs, cursor, cursor, e);
            }
        };
    }


    /**
     * Log a warning regarding the {@link TwitterException} {@code e} occurring
     * when talking with Twitter about Twitter user {@code id}. Where possible,
     * this customises the error message to match the nature of the error, rather
     * than simply spamming the logs with exception stacktraces.
     *
     * @param id The ID of the Twitter user used in the Twitter API call that generated <code>e</code>.
     * @param e The {@link TwitterException} that occurred when talking to the Twitter API.
     */
    private void logTwitterApiException(Long id, TwitterException e) {
        if (e.getStatusCode() == HttpResponseCode.UNAUTHORIZED) {
            LOG.warn("Account {} is protected and innaccessible", id);
        } else {
            LOG.warn("Barfed parsing JSON or saving to file info related to {}.", id, e);
        }
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
        System.err.println("Current mutex keys: " + limitsMutex.keySet().stream().collect(Collectors.joining(",")));
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

            if (resp.hasARateLimitStatusUpdate()) {
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
            "friends",    // endpoint family for "/friends/ids"
            "search" // endpoint for searches for mentions
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
     * Reads Twitter IDs (expected to be longs) from the given array of
     * ID strings ({@link Config#ids}) if provided, otherwise it reads them
     * from {@link Config#idsFile} into a list. IDs are expected in the file
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
     * @param config Config object with an <code>ids</code> array or an <code>idsFile</code>
     *          file path to a file with Twitter IDs (long values).
     * @return A list of Twitter IDs.
     * @throws IOException If there's an issue reading the file.
     * @throws NumberFormatException If there's an issue parsing the IDs.
     */
    private Set<Long> loadIDs(final Config config) throws IOException {
        if (config.ids != null && config.ids.length > 0) {
            return Arrays.stream(config.ids).map(Long::parseLong).collect(Collectors.toSet());
        } else {
            return Files.readAllLines(Paths.get(config.idsFile)).stream()
                .map(l -> l.split("#")[0].trim())
                .filter(l -> l.length() > 0 && ! l.startsWith("#"))
                .map(Long::parseLong)
                .collect(Collectors.toSet());
        }
    }

    /**
     * Builds the {@link Configuration} object with which to connect to Twitter,
     * including credentials and looks for proxy information in
     * <code>proxy.properties</code> if it's present.
     *
     * @param credentialsFile Property file of Twitter credentials.
     * @param debug Debug flag to pass to Twitter4j's config builder.
     * @return a Twitter4j {@link Configuration} object.
     * @throws IOException if there's an error loading the application's {@code credentialsFile}.
     */
    private static Configuration twitterConfig(
        final String credentialsFile, final boolean debug
    ) throws IOException {

        credentials = loadCredentials(credentialsFile);

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

        Path credsPath = Paths.get(credentialsFile);
        if (credentialsFile.equals(DEFAULT_CREDENTIALS_PATH)) {
            credsPath = checkPath(credsPath);
        }

        properties.load(Files.newBufferedReader(credsPath));

        return properties;
    }


    /**
     * Examines the path (assumed to be to a file in the current directory) and checks
     * if it exists, and resorts to looking in the user's home directory for a corresponding
     * file if it does not. Returns the original, or the updated, path if a file is found
     * and throws a {@link RuntimeException} if not.
     *
     * @param file The path to the local file to check.
     * @return A valid path to an existing file.
     * @throws RuntimeException if the file does not exist in the local or home directories.
     */
    private static Path checkPath(final Path file) {
        if (! file.toFile().exists()) {
            final String homePath = System.getProperty("user.home", ".") + FILE_SEPARATOR;
            final String altPath = homePath + file.getFileName();

            LOG.warn("No file at {}. Trying {}.", file, altPath);

            final Path newPath = Paths.get(altPath);
            if (! newPath.toFile().exists()) {
                throw new RuntimeException(
                    "Cannot find file at " + file + " nor alternative " + altPath
                );
            }
            LOG.info("Found file at {}", newPath);
            return newPath;
        }
        return file;
    }


    /**
     * Loads proxy properties from {@code ./proxy.properties} or {@link ~/proxy.properties}
     * and, if a password is not supplied, asks for it in the console.
     *
     * @return A Properties instance filled with proxy information.
     */
    private static Properties loadProxyProperties() {
        final Properties properties = new Properties();
        Path proxyPath = /*checkPath*/(Paths.get("./proxy.properties"));
        if (proxyPath.toFile().exists()) {
            boolean success = true;
            try (Reader fileReader = Files.newBufferedReader(proxyPath)) {
                properties.load(fileReader);
            } catch (final IOException e) {
                System.err.printf("Attempted and failed to load %s: %s\n", proxyPath, e.getMessage());
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
