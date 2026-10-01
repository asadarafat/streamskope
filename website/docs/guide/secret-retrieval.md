# Reuse connection secrets

A retrieval preset is an optional recipe for filling Kafka connection
fields. It describes where to retrieve a certificate or truststore, how to get
its password, and which OAuth settings to suggest.

Start with the [Kafka profile editor](connections.md#configure-manually) and a
saved recipe supplied by your team. Manual setup works without a recipe.

## Use a recipe

1. In the Kafka profile editor, choose a trust format.
2. Expand **Retrieve certificates and credentials** and choose a saved recipe from **Retrieval preset**.
3. Enter its required parameters and temporary access credentials.
4. Choose **Retrieve**. For SSH, review the server identity before authentication.
5. Review the retrieved certificate details and choose which suggested OAuth settings to apply.
6. Choose **Apply to connection** to fill the draft certificate, any retrieved truststore password and the selected OAuth fields.
7. Choose **Test connection** and confirm the connection test passes.
8. Choose **Save profile**, or **Update profile** when editing an existing profile.

**You should see:** the reviewed trust material applied to the connection draft,
followed by a successful connection test before saving.
The field indicates when a truststore password has been retrieved. The host
retains the secret; the result summary does not expose its value.

## Create or edit a recipe

1. Expand **Retrieve certificates and credentials** in the profile editor and choose **Manage…** beside **Retrieval preset**.
2. Give the recipe a useful name and choose its trust format and retrieval method.
3. Configure that method's fields using the table below.
4. Save the recipe, then select it in the Kafka profile editor and follow **Use a recipe** above.

| Method             | Configure                                              | Key limitation                                      |
| ------------------ | ------------------------------------------------------ | --------------------------------------------------- |
| SSH file           | Host parameters and remote certificate path            | The account must be able to read the file           |
| SSH command output | Command that writes the material to stdout             | Do not mix progress messages with certificate bytes |
| HTTPS              | GET endpoint, extraction format and API authentication | TLS trust must already be established               |

HTTPS supports raw bytes, JSON PEM and JSON base64 extraction, with None, Basic
or Bearer authentication. Redirects, plaintext HTTP, API OAuth login, mTLS and
response scripts are unsupported. Password requests must use the material
request's origin.

Saving or importing a recipe does not execute it. Review commands before running
them and keep secrets out of URLs, commands and non-secret defaults.

## Changes and failures

Saved profiles pin a recipe revision. Editing a recipe does not silently change
a working profile. Adopt changes explicitly. There is no background retrieval.

Closing the editor cancels pending work. Temporary access credentials must be
entered again. On a partial failure, saved trust remains unchanged; use the
operation's correlation ID in **Raw logs** to identify what failed.

## Next: connect

[Test and connect the Kafka profile →](connections.md#configure-manually)
