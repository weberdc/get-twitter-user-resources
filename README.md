# Fetch Twitter profiles in batches

Author: **Derek Weber** (with many thanks to [http://twitter4j.org]() examples)

Last updated: **2016-09-27**

App to retrieve Twitter profiles in batches.

Requirements:
 + Java Development Kit 1.8
 + [twitter4j-core](http://twitter4j.org) (Apache 2.0 licence)
   + depends on [JSON](http://json.org) ([JSON licence](http://www.json.org/license.html))
 + [FasterXML](http://wiki.fasterxml.com/JacksonHome) (Apache 2.0 licence)
 + [Google Guava](https://github.com/google/guava) (Apache 2.0 licence)
 + [Commons CLI](https://commons.apache.org/cli) (Apache 2.0 licence)

Built with [Gradle 3.0](http://gradle.org).

## To Build

The Gradle wrapper has been included, so it's not necessary for you to have Gradle
installed - it will install itself as part of the build process. All that is required is
the Java Development Kit.

By running

`$ ./gradlew installDist` or `$ gradlew.bat installDist`

you will create an installable copy of the app in `PROJECT_ROOT/build/get-twitter-profiles`.

Use the target `distZip` to make a distribution in `PROJECT_ROOT/build/distributions`
or the target `timestampedDistZip` to add a timestamp to the distribution archive filename.

To also include your own local `twitter.properties` file with your Twitter credentials,
use the target `privilegedDistZip` to make a special distribution in
`PROJECT_ROOT/build/distributions` that includes the current credentials.


## Configuration

Twitter OAuth credentials must be available in a properties file based on the
provided `twitter.properties-template` in the project's root directory. Copy the
template file to a properties file (the default is `twitter.properties` in the same
directory), and edit it with your Twitter app credentials. For further information see
[http://twitter4j.org/en/configuration.html]().

If running the app behind a proxy or filewall, copy the `proxy.properties-template`
file to a file named `proxy.properties` and set the properties inside to your proxy
credentials. If you feel uncomfortable putting your proxy password in the file, leave
the password-related ones commented and the app will ask for the password.

## Usage
If you've just downloaded the binary distribution, do this from within the unzipped
archive (i.e. in the `get-twitter-profiles` directory). Otherwise, if you've just built
the app from source, do this from within `PROJECT_ROOT/build/install/get-twitter-profiles`:
<pre>
Usage: bin/get-twitter-profiles[.bat] [options]
  Options:
    -c, --credentials
       Properties file with Twitter OAuth credentials
       Default: ./twitter.properties
    -i, --ids-file
       Path to a file with Twitter screen names, one per line
       Default: none
    -o, --output-directory
       Path to a directory into which to write the fetched profiles
       Default: ./output
    -h, --help
       Print usage instructions
       Default: false
    -debug
       Debug mode
       Default: false
</pre>

Run the app with the list of screen names you wish to fetch:
<pre>
prompt> bin/get-twitter-profiles --ids-list screennames.txt -o path/to/profiles -debug
</pre>

This will create a directory `path/to/profiles` and create a file for each
profile downloaded, with the filename `profile-<profile-id>.json`. Profiles are
fetched in batches of 100, which is the limit applied by Twitter's relevant
[API call](https://dev.twitter.com/rest/reference/get/users/lookup).

Attempts have been made to account for Twitter's rate limits, so at times the
app will pause, waiting until the rate limit has refreshed. It reports how long
it will wait when it does have to pause.
