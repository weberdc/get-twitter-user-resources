# Fetch resources associated with each of a list of Twitter IDs

Author: **Derek Weber** (with many thanks to [Twitter4J](http://twitter4j.org)
for their examples)

Last updated: **2016-10-08**

App to retrieve resources associated with Twitter accounts, such as tweets and
favourites.

Requirements:
 + Java Development Kit 1.8
 + [twitter4j-core](http://twitter4j.org) (Apache 2.0 licence)
   + depends on [JSON](http://json.org) ([JSON licence](http://www.json.org/license.html))
 + [FasterXML](http://wiki.fasterxml.com/JacksonHome) (Apache 2.0 licence)
 + [Google Guava](https://github.com/google/guava) (Apache 2.0 licence)
 + [Commons CLI](https://commons.apache.org/cli) (Apache 2.0 licence)
 + [SLF4J](http://www.slf4j.org/) (MIT licence)
 + [log4j 1.2](https://logging.apache.org/log4j/1.2/) (Apache 2.0 licence)

Built with [Gradle 3.0](http://gradle.org).

## To Build

The Gradle wrapper has been included, so it's not necessary for you to have Gradle
installed - it will install itself as part of the build process. All that is required is
the Java Development Kit.

By running

`$ ./gradlew installDist` or `$ gradlew.bat installDist`

you will create an installable copy of the app in `PROJECT_ROOT/build/get-twitter-user-resources`.

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
archive (i.e. in the `get-twitter-user-resources` directory). Otherwise, if you've
just built the app from source, do this from within
`PROJECT_ROOT/build/install/get-twitter-user-resources`:
<pre>
usage: TwitterUserResourcesRetrieverApp
 -c,--credentials <arg>        File of Twitter credentials (default:
                               ./twitter.properties)
 -d,--debug                    Turn on debugging information (default:
                               false)
    --favourites               Collect favourites
    --followers                Collect follower IDs
    --friends                  Collect friend (followee) IDs
 -h,--help                     Ask for help with using this tool.
 -i,--identifiers-file <arg>   File of Twitter screen names
 -o,--output-directory <arg>   Directory to which to write profiles
                               (default: ./output)
    --tweets                   Collect statuses (tweets)
</pre>

Run the app with the list of Twitter IDs you wish to fetch:
<pre>
prompt> bin/get-twitter-user-resources -i twitter_ids.txt -o path/to/output --tweets --favourites
</pre>

This will create a directory `path/to/output` and create files for each of the
resource types fetched for each of the Twitter IDs, with the filenames
`<resource-type>-<profile-id>.json` (e.g. `tweets-123.json` and `favourites-123.json`).
Resources must be fetched as separate API calls for each resource type for each
Twitter ID - no batch fetching facility is available - therefore, for a modest
number of IDs, Twitter's rate limits may be invoked quickly, and the app will
slow down as it deals with them.
