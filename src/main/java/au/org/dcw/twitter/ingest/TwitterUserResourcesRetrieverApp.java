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
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import twitter4j.RateLimitStatus;
import twitter4j.RateLimitStatusEvent;
import twitter4j.RateLimitStatusListener;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Application that collects the profiles of Twitter users.
 * <p>
 *
 * @see <a href=
 *      "https://github.com/yusuke/twitter4j/blob/master/twitter4j-examples/src/main/java/twitter4j/examples/json/SaveRawJSON.java">SaveRawJSON.java</a>
 * @see <a href=
 *      "https://dev.twitter.com/rest/reference/get/users/lookup">Twitter's
 *      <code>GET users/lookup</code> endpoint</a>
 */
public final class TwitterUserResourcesRetrieverApp {
    private static Logger LOG = LoggerFactory.getLogger(TwitterUserResourcesRetrieverApp.class);
    private static final int FETCH_BATCH_SIZE = 100;
    private static final Options OPTIONS = new Options();
    static {
        OPTIONS.addOption("i", "ids-file", true, "File of Twitter screen names");
        OPTIONS.addOption("o", "output-directory", true, "Directory to which to write profiles (default: ./profiles)");
        OPTIONS.addOption("c", "credentials", true, "File of Twitter credentials (default: ./twitter.properties)");
        OPTIONS.addOption("d", "debug", false, "Turn on debugging information (default: false)");
        OPTIONS.addOption("?", "help", false, "Ask for help with using this tool.");
    }

    /**
     * Prints how the app ought to be used and causes the VM to exit.
     */
    private static void printUsageAndExit() {
        new HelpFormatter().printHelp("TwitterUserResourcesRetrieverApp", OPTIONS);
        System.exit(0);
    }

    public static void main(String[] args) throws IOException {
        final CommandLineParser parser = new BasicParser();
        String screenNamesFile = null;
        String outputDir = "./output";
        String credentialsFile = "./twitter.properties";
        boolean debug = false;
        try {
            final CommandLine cmd = parser.parse(OPTIONS, args);
            if (cmd.hasOption('i')) screenNamesFile = cmd.getOptionValue('i');
            if (cmd.hasOption('o')) outputDir = cmd.getOptionValue('o');
            if (cmd.hasOption('c')) credentialsFile = cmd.getOptionValue('c');
            if (cmd.hasOption('d')) debug = true;
            if (cmd.hasOption('h')) printUsageAndExit();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        // check config
        if (screenNamesFile == null) {
            printUsageAndExit();
        }

        new TwitterUserResourcesRetrieverApp().run(screenNamesFile, outputDir, credentialsFile, debug);
    }

    /**
     * Fetch the profiles specified by the screen names in the given {@code screenNamesFile}
     * and write the profiles to the given {@code outputDir}.
     *
     * @param screenNamesFile Path to file with screen names, one per line.
     * @param outputDir Path to directory into which to write fetched profiles.
     * @param credentialsFile Path to properties file with Twitter credentials.
     * @param debug Whether to increase debug logging.
     */
    public void run(
        final String screenNamesFile,
        final String outputDir,
        final String credentialsFile,
        final boolean debug
    ) throws IOException {

        LOG.info("Collecting profiles");
        LOG.info("  Twitter screen names: " + screenNamesFile);
        LOG.info("  output directory: " + outputDir);

        final List<String> screenNames = this.loadScreenNames(screenNamesFile);

        LOG.info("Read {} screen names", screenNames.size());

        if (!Files.exists(Paths.get(outputDir))) {
            LOG.info("Creating output directory {}", outputDir);
            Paths.get(outputDir).toFile().mkdirs();
        }

        final Configuration config =
            TwitterUserResourcesRetrieverApp.buildTwitterConfiguration(credentialsFile, debug);
        final Twitter twitter = new TwitterFactory(config).getInstance();
        twitter.addRateLimitStatusListener(this.rateLimitStatusListener);
        final ObjectMapper json = new ObjectMapper();

        for (final List<String> batch : Lists.partition(screenNames, FETCH_BATCH_SIZE)) {

            try {
                LOG.info("Looking up {} users' profiles", batch.size());

                // ask Twitter for profiles
                final String[] allocation = new String[batch.size()];
                final ResponseList<User> profiles = twitter.lookupUsers(batch.toArray(allocation));

                // extract the raw JSON
                final String rawJsonProfiles = TwitterObjectFactory.getRawJSON(profiles);

                // traverse the raw JSON (rather than the Twitter4j structures)
                final JsonNode profilesJsonNode = json.readTree(rawJsonProfiles);
                profilesJsonNode.forEach (profileNode ->  {

                    // extract vars for logging and filename creation
                    final String profileId = profileNode.get("id_str").asText();
                    final String screenName = profileNode.get("screen_name").asText();

                    final String fileName = outputDir + "/profile-" + profileId + ".json";

                    LOG.info("Profile @{} {} -> {}", screenName, profileId, fileName);
                    try {
                        saveJSON(profileNode.toString(), fileName);
                    } catch (IOException e) {
                        LOG.warn("Failed to write to {}", fileName, e);
                    }
                });

            } catch (TwitterException e) {
                LOG.warn("Failed to communicate with Twitter", e);
            }
        }
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
