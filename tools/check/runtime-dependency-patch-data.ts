// Exact socket lifecycle and TLS identity corrections, not an advisory exemption.
// Retire after an upstream release passes the public socket and TLS/auth regressions.
export const RUNTIME_DEPENDENCY_PATCHES = [
  {
    name: "@nats-io/transport-node",
    version: "3.4.0",
    rationaleUrl:
      "https://github.com/nats-io/nats.js/blob/95e76e79d9feaa0a0bf3b0e8da526ec5a3460979/transport-node/src/node_transport.ts",
    registryUrl: "https://registry.npmjs.org/@nats-io/transport-node/-/transport-node-3.4.0.tgz",
    integrity:
      "sha512-hH7u7ejIBTFEJIZ8rIcMrHJI6wl+HhpO5sVFs1+ppmXa8RuB2+Lh1+UwTzZ5xTNNm1TKcRkYy+2qCV56qp8RxA==",
    files: [
      {
        file: "lib/node_transport.js",
        originalSha256: "527474ca776b69b5ba35e5be54f23da838280b91f52d508423baf47b79b55fec",
        patchedSha256: "9d02ac35e7a347030ddd5ff93dfc3035d4294b89c5009d3367ae5fff44ccdda5",
        replacements: [
          {
            before: '    tlsName = "";\n    done = false;',
            after: '    tlsName = "";\n    tlsHost = "";\n    done = false;',
          },
          {
            before: "        this.tlsName = hp.tlsName;\n        this.options = options;",
            after:
              "        this.tlsName = hp.tlsName;\n        this.tlsHost = hp.tlsName || hp.hostname;\n        this.options = options;",
          },
          {
            before:
              "        let tlsOpts = {\n            socket: this.socket,\n            servername: this.tlsName,\n            rejectUnauthorized: true,\n        };",
            after:
              "        let tlsOpts = {\n            socket: this.socket,\n            servername: this.tlsName,\n            host: this.tlsHost,\n            rejectUnauthorized: true,\n        };",
          },
          {
            before:
              "        // if this connection didn't succeed, then ignore it.\n        if (!this.connected)\n            return;",
            after:
              "        // Setup sockets remain owned even before INFO/TLS completes.\n        if (!this.connected) {\n            this.done = true;\n            this.closeError = err;\n            // Keep handshake close/error listeners so pending setup rejects too.\n            this.socket?.destroy(err);\n            return;\n        }",
          },
          {
            before: '        socket.on("error", (err) => {\n            dialError = err;',
            after:
              '        this.socket = socket;\n        socket.on("error", (err) => {\n            dialError = err;',
          },
          {
            before:
              "            const tlsSocket = (0, node_tls_1.connect)(hp.port, hp.hostname, tlsOpts, () => {\n                tlsSocket.removeAllListeners();\n                d.resolve(tlsSocket);\n            });",
            after:
              "            const tlsSocket = (0, node_tls_1.connect)(hp.port, hp.hostname, tlsOpts, () => {\n                tlsSocket.removeAllListeners();\n                d.resolve(tlsSocket);\n            });\n            this.socket = tlsSocket;",
          },
          {
            before:
              "            const tlsSocket = (0, node_tls_1.connect)(tlsOpts, () => {\n                tlsSocket.removeAllListeners();\n                d.resolve(tlsSocket);\n            });",
            after:
              "            const tlsSocket = (0, node_tls_1.connect)(tlsOpts, () => {\n                tlsSocket.removeAllListeners();\n                d.resolve(tlsSocket);\n            });\n            this.socket = tlsSocket;",
          },
        ],
      },
    ],
  },
] as const;
