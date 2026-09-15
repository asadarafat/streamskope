# Connect your Kafka

Save a connection profile, test it, and open your cluster's topics.
A profile keeps the settings for one Kafka connection together.

Trying StreamSkope for the first time? The [local quickstart](../start/quickstart.md)
sets up a broker and profile for you.

## Before you start

Have your bootstrap broker addresses, trust material and authentication settings
ready. Broker addresses must be reachable from the machine running the
StreamSkope host. For OAuth, you also need the token endpoint, client ID, secret
and any required scope.

## Configure manually

1. Open **Connection Profiles**, then **Add profile**.
2. Enter a **Profile name** you will recognize and the **Bootstrap brokers**.
3. Choose **Trust material format**: PEM, JKS or PKCS12. Select the matching
   certificate or truststore file and enter its password if required.
4. If your cluster uses OAuth, enable **Use OAuth OAUTHBEARER** and enter its
   token endpoint, client ID, client secret and scope.
5. Click **Test connection**. Wait for the result. If it fails, open **Raw logs**
   and resolve the reported connection, certificate or authentication problem.
6. Click **Save profile**. Find the saved profile and click its connect/play button.
7. Wait for **Connected**, then open **Topics**.

**You should see:** the topics your account can access. Testing checks the supplied
settings; you still need to save and connect to use them.

![Connection profiles in StreamSkope](../assets/profiles.png#only-light)
![Connection profiles in StreamSkope dark mode](../assets/profiles-dark.png#only-dark)

## Next: inspect a message

[Open a topic and read a record →](messages.md)

If you cannot connect, follow [Fix a connection problem](troubleshooting.md).
For Schema Registry, [configure and check its separate service connection](schema-registry.md).

## Automate repeated setup

Use a [Secret Retrieval Profile](secret-retrieval.md) to retrieve trust material
and suggest OAuth settings, then test and save the connection as above.

## Update or recover

1. Open the saved profile's editor and change the settings you need.
2. Test the edited settings before updating the profile.
3. Reconnect and confirm that the expected topics are available.

Editing non-secret settings retains protected values unless you replace or clear
them. Desktop secrets use the operating-system credential service; browser
development profiles are session-only.

For desktop recovery, quit the app and back up `profiles/kafka-profiles.json`
and its `.pre-upgrade.bak` under the application-data directory. Restore a
known-good backup with the same OS user and credential context, using the matching
earlier app. Do not edit store versions or decrypt files to bypass protection.
