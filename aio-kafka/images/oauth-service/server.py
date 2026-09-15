import base64
import datetime
import os

import jwt
from flask import Flask, jsonify, request
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

app = Flask(__name__)

# For Kafka's validator, prefer RS256 (asymmetric) over HS256.
# Generate an in-memory RSA keypair at startup and expose the public key via JWKS.
_rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_rsa_priv_pem = _rsa_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)
_rsa_pub = _rsa_key.public_key().public_numbers()
KID = "dummy-rs256"
CLIENT_ID = os.environ["OAUTH_CLIENT_ID"]
CLIENT_SECRET = os.environ["OAUTH_CLIENT_SECRET"]
OAUTH_SCOPE = os.environ["OAUTH_SCOPE"]
OAUTH_ISSUER = os.environ["OAUTH_ISSUER"]
SCHEMA_REGISTRY_AUDIENCE = os.environ["SCHEMA_REGISTRY_AUDIENCE"]
SCHEMA_REGISTRY_ROLE = os.environ["SCHEMA_REGISTRY_ROLE"]


@app.route("/rest-gateway/rest/api/v1/auth/token", methods=["POST"])
def token():
    # Accept credentials via Basic auth or form fields (client_id/client_secret or clientId/clientSecret)
    cid = None
    cs = None
    # Basic auth header
    auth = request.headers.get("Authorization")
    if auth and auth.lower().startswith("basic "):
        try:
            userpass = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
            cid, cs = userpass.split(":", 1)
        except Exception:
            pass
    # Form fields fallback
    if not cid:
        cid = request.form.get("client_id") or request.form.get("clientId")
    if not cs:
        cs = request.form.get("client_secret") or request.form.get("clientSecret")

    if cid != CLIENT_ID or cs != CLIENT_SECRET:
        return jsonify({"invalid_client": True}), 401

    payload = {
        "aud": SCHEMA_REGISTRY_AUDIENCE,
        "iss": OAUTH_ISSUER,
        "roles": [SCHEMA_REGISTRY_ROLE],
        "sub": CLIENT_ID,
        "scope": OAUTH_SCOPE,
        "iat": datetime.datetime.now(datetime.UTC),
        "exp": datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=10),
    }

    headers = {"kid": KID, "alg": "RS256"}
    tok = jwt.encode(payload, _rsa_priv_pem, algorithm="RS256", headers=headers)
    return jsonify({"access_token": tok, "expires_in": 600, "token_type": "bearer"})


@app.route("/.well-known/jwks.json", methods=["GET"])
def jwks():
    # Publish RSA public key as JWK
    def b64u(n: int) -> str:
        # int -> bytes (big endian) -> base64url (no padding)
        b = n.to_bytes((n.bit_length() + 7) // 8, byteorder="big")
        return base64.urlsafe_b64encode(b).decode("utf-8").rstrip("=")

    jwk = {
        "kty": "RSA",
        "n": b64u(_rsa_pub.n),
        "e": b64u(_rsa_pub.e),
        "alg": "RS256",
        "kid": KID,
        "use": "sig",
    }
    return jsonify({"keys": [jwk]})


if __name__ == "__main__":
    # Bind to 0.0.0.0 so other containers can reach it
    app.run(host="0.0.0.0", port=5000)
