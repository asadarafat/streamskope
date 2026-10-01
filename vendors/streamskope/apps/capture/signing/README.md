# EDA application signing

Production publication uses an encrypted EDABuilder signing key. Generate the
key pair once with the pinned EDABuilder version:

```text
SIGNING_KEY_PASSWORD=<strong-password> edabuilder sign generate-key-pair \
  --private-key streamskope-eda.key \
  --public-key streamskope-eda.pub
```

Commit only `streamskope-eda.pub` in this directory. Store the base64-encoded
private key and its password as the protected `eda-production` environment
secrets `EDA_APP_SIGNING_KEY_B64` and `EDA_APP_SIGNING_KEY_PASSWORD`.

An EDA administrator must add the committed public key as a trusted EDA
`SigningKey` before installing StreamSkope Capture. Never commit the private key
or its password.
