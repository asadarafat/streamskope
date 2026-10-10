import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { request as httpRequest, type IncomingHttpHeaders } from "node:http";
import { join } from "node:path";

interface Reply {
  readonly status: number;
  readonly headers: IncomingHttpHeaders;
  readonly body: string;
}
function record(value: unknown): Record<string, unknown> {
  assert.ok(value !== null && typeof value === "object" && !Array.isArray(value));
  return value as Record<string, unknown>;
}
function docker(args: readonly string[], input: string): string {
  const result = spawnSync("docker", [...args], {
    input,
    encoding: "utf8",
    timeout: 60_000,
    maxBuffer: 64 * 1024,
  });
  assert.ok(
    result.error === undefined && result.status === 0,
    "Native worker qualification failed.",
  );
  return result.stdout.trim();
}
function request(
  port: number,
  path: string,
  value?: unknown,
  cookie?: string,
  origin?: string,
): Promise<Reply> {
  const body = value === undefined ? undefined : JSON.stringify(value);
  return new Promise((resolve, reject) => {
    const request = httpRequest(
      {
        hostname: "127.0.0.1",
        port,
        path,
        method: body === undefined ? "GET" : "POST",
        headers: {
          origin: origin ?? `http://127.0.0.1:${port}`,
          ...(body === undefined
            ? {}
            : { "content-type": "application/json", "content-length": Buffer.byteLength(body) }),
          ...(cookie === undefined ? {} : { cookie }),
        },
      },
      (response) => {
        const parts: Buffer[] = [];
        let size = 0;
        response.on("data", (chunk: Buffer) => {
          size += chunk.length;
          if (size > 4 * 1024 * 1024)
            response.destroy(new Error("Container qualification response limit."));
          else parts.push(chunk);
        });
        response.once("error", reject);
        response.once("end", () =>
          resolve({
            status: response.statusCode ?? 0,
            headers: response.headers,
            body: Buffer.concat(parts).toString("utf8"),
          }),
        );
      },
    );
    request.once("error", reject);
    request.setTimeout(10_000, () =>
      request.destroy(new Error("Container qualification request timeout.")),
    );
    request.end(body);
  });
}
function cookieFrom(reply: Reply): string {
  const cookie = reply.headers["set-cookie"]?.[0];
  assert.ok(cookie !== undefined);
  assert.ok(cookie.includes("HttpOnly") && cookie.includes("SameSite=Strict"));
  return cookie.split(";")[0]!;
}

export async function waitForBrowser(port: number): Promise<void> {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    try {
      if ((await request(port, "/health")).status === 200) return;
    } catch {
      /* Starting listener. */
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("Container qualification listener did not become ready.");
}

export interface BrowserVaultFixture {
  /** Check never restarts or authenticates again, so it proves a read-only check kept the session. */
  verifyUnlocked(): Promise<void>;
  unlockAfterReplacement(): Promise<void>;
  lock(): Promise<void>;
  assertNoSecrets(text: string): void;
  /** Uses this fixture's authenticated session and the running Kafka host's own protocol. */
  kafkaCommand(name: string, payload: unknown): Promise<Record<string, unknown>>;
  /** Independent encrypted interrupted-job fixture; call only after locking this owned host. */
  seedRepairHistory(container: string): void;
}

/** Credentials and exact expected profile remain owned by this disposable fixture's closure. */
export async function createBrowserVaultFixture(instance: {
  readonly port: number;
  readonly data: string;
}): Promise<BrowserVaultFixture> {
  const { port, data } = instance;
  const passphrase = `isolated qualification ${randomUUID()}`;
  const secret = `qualification-${randomUUID()}`;
  const assertNoSecrets = (text: string): void => {
    assert.ok(
      !text.includes(secret) && !text.includes(passphrase),
      "Qualification exposed private fixture data.",
    );
  };
  await waitForBrowser(port);
  assert.equal(record(JSON.parse((await request(port, "/health")).body)).status, "locked");
  assert.equal((await request(port, "/")).status, 303);
  assert.equal((await request(port, "/__streamskope_host/providers/nats/health")).status, 401);
  const setupCode = (await readFile(join(data, "setup-code"), "utf8")).trim();
  assert.equal(
    (
      await request(
        port,
        "/__streamskope_session/create",
        { passphrase, setupCode },
        undefined,
        "http://invalid.example.test",
      )
    ).status,
    403,
  );
  const created = await request(port, "/__streamskope_session/create", { passphrase, setupCode });
  assert.equal(created.status, 200);
  let cookie = cookieFrom(created);
  let protocolVersion: number;
  const refreshProtocol = async (): Promise<void> => {
    const health = await request(
      port,
      "/__streamskope_host/providers/nats/health",
      undefined,
      cookie,
    );
    assert.equal(health.status, 200);
    const version: unknown = record(JSON.parse(health.body)).protocolVersion;
    assert.ok(typeof version === "number" && Number.isSafeInteger(version) && version > 0);
    protocolVersion = version;
  };
  await refreshProtocol();
  assert.equal((await request(port, "/", undefined, cookie)).status, 200);
  assert.ok(
    (
      await request(port, "/__streamskope_session/browser-runtime.js", undefined, cookie)
    ).body.includes("lockVault"),
  );
  const command = async (name: string, payload: unknown): Promise<Record<string, unknown>> => {
    const reply = await request(
      port,
      "/__streamskope_host/providers/nats/commands",
      {
        id: randomUUID(),
        version: protocolVersion,
        command: name,
        payload,
      },
      cookie,
    );
    assert.equal(reply.status, 200);
    assertNoSecrets(reply.body);
    const body = record(JSON.parse(reply.body));
    assert.equal(body.ok, true);
    return record(body.result);
  };
  const createdProfile = await command("profiles.create", {
    profile: {
      name: "Disposable container qualification",
      servers: ["nats://remote.example.test:4222"],
      authentication: { mode: "token", token: { mode: "replace", value: secret } },
      tls: { mode: "plaintext" },
    },
  });
  const profiles: unknown = record(createdProfile.profiles).profiles;
  assert.ok(Array.isArray(profiles) && profiles.length === 1);
  const expected = structuredClone(record(profiles[0]));
  assert.equal(typeof expected.id, "string");
  assert.equal(record(expected.authentication).tokenPresent, true);
  for (const filename of ["vault.json", "nats-profiles.json"])
    assertNoSecrets(await readFile(join(data, filename), "utf8"));
  const verifyUnlocked = async (): Promise<void> => {
    const restored: unknown = record((await command("profiles.list", {})).profiles).profiles;
    assert.deepEqual(
      restored,
      [expected],
      "The exact encrypted profile must survive host replacement.",
    );
  };
  assert.equal((await request(port, "/__streamskope_session/lock", {}, cookie)).status, 200);
  assert.equal(
    (await request(port, "/__streamskope_host/providers/nats/health", undefined, cookie)).status,
    401,
  );
  assert.equal(
    (
      await request(port, "/__streamskope_session/unlock", {
        passphrase: "wrong qualification passphrase",
      })
    ).status,
    401,
  );
  const unlock = async (): Promise<void> => {
    const unlocked = await request(port, "/__streamskope_session/unlock", { passphrase });
    assert.equal(unlocked.status, 200);
    cookie = cookieFrom(unlocked);
    await refreshProtocol();
    await verifyUnlocked();
  };
  await unlock();
  return {
    verifyUnlocked,
    assertNoSecrets,
    seedRepairHistory: (container): void => {
      // The disposable passphrase stays in this closure and enters the owned worker
      // through stdin. Neither it nor protected contents enter a qualification receipt.
      const result = docker(
        ["exec", "--interactive", container, "node"],
        `
        const fs=require('node:fs'); const crypto=require('node:crypto'); const assert=require('node:assert/strict');
        (async()=>{
          const metadata=JSON.parse(fs.readFileSync('/data/vault.json','utf8'));
          const key=await new Promise((resolve,reject)=>crypto.scrypt(${JSON.stringify(passphrase)},Buffer.from(metadata.salt,'base64'),32,{...metadata.kdf,maxmem:64*1024*1024},(error,key)=>error?reject(error):resolve(key)));
          const aad=Buffer.from(JSON.stringify({version:metadata.version,cipher:metadata.cipher,kdf:metadata.kdf,salt:metadata.salt,vaultId:metadata.vaultId}));
          const valueAad=Buffer.concat([aad,Buffer.from('\\0profile-value')]);
          const original={state:'complete',encoding:'base64',key:null,value:Buffer.from('disposable native repair').toString('base64'),headers:[]};
          const input={targetProfile:null,topic:'native-repair',partition:0,ratePerSecond:1,records:[{topic:'native-source',partition:0,offset:'0',timestampMs:null,original}],transform:{key:null,removeHeaders:[],appendHeaders:[],valueText:null}};
          const timestamp=new Date().toISOString();
          const review={planId:'native-interrupted-repair',sourceName:'Disposable source',targetName:'Disposable target',expiresAt:timestamp,input,batch:{topic:input.topic,partition:0,ratePerSecond:1,records:[original],timestamps:[null]},destination:{clusterId:'fixture-cluster',topicId:'fixture-topic',partitions:1}};
          const document={schemaVersion:1,jobs:[{id:review.planId,createdAt:timestamp,updatedAt:timestamp,review,outcomes:[],pendingIndex:0,status:'running',cleanup:'pending'}]};
          const nonce=crypto.randomBytes(12),cipher=crypto.createCipheriv('aes-256-gcm',key,nonce);cipher.setAAD(valueAad);
          const encrypted=Buffer.concat([cipher.update(JSON.stringify(document),'utf8'),cipher.final()]);
          const value=Buffer.concat([Buffer.from('SKV1'),nonce,cipher.getAuthTag(),encrypted]).toString('base64');key.fill(0);
          fs.mkdirSync('/data/history',{recursive:true,mode:0o700}); const path='/data/history/kafka-repair-jobs.json';assert.equal(fs.existsSync(path),false);
          const fd=fs.openSync(path,'wx',0o600);try{fs.writeFileSync(fd,JSON.stringify({schemaVersion:1,protected:value})+'\\n');fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
          const directory=fs.openSync('/data/history','r');try{fs.fsyncSync(directory);}finally{fs.closeSync(directory);}
          console.log('native-repair-fixture-seeded');
        })().catch(()=>{process.exitCode=1;});
      `,
      );
      assert.equal(result, "native-repair-fixture-seeded");
    },
    kafkaCommand: async (name, payload): Promise<Record<string, unknown>> => {
      const health = await request(
        port,
        "/__streamskope_host/providers/kafka/health",
        undefined,
        cookie,
      );
      assert.equal(health.status, 200);
      const version: unknown = record(JSON.parse(health.body)).protocolVersion;
      assert.ok(typeof version === "number" && Number.isSafeInteger(version) && version > 0);
      const reply = await request(
        port,
        "/__streamskope_host/providers/kafka/commands",
        {
          id: randomUUID(),
          version,
          command: name,
          payload,
        },
        cookie,
      );
      assert.equal(reply.status, 200);
      assertNoSecrets(reply.body);
      const body = record(JSON.parse(reply.body));
      assert.equal(body.ok, true, "The native Kafka fixture command must be accepted.");
      return record(body.result);
    },
    lock: async (): Promise<void> => {
      assert.equal((await request(port, "/__streamskope_session/lock", {}, cookie)).status, 200);
    },
    unlockAfterReplacement: async (): Promise<void> => {
      await waitForBrowser(port);
      assert.equal(record(JSON.parse((await request(port, "/health")).body)).status, "locked");
      await unlock();
    },
  };
}

export function verifyBrowserNativeWorkers(container: string): void {
  const worker = docker(
    ["exec", "--interactive", container, "node"],
    `
      const {Worker}=require('node:worker_threads'); const assert=require('node:assert/strict'); const fs=require('node:fs');
      (async()=>{
        const run=(name,workerData)=>new Promise((resolve,reject)=>{const w=new Worker(process.cwd()+'/dist/web/'+name+'.cjs',{workerData});w.once('message',resolve);w.once('error',reject);});
        const trust=await run('trust-material-worker',{kind:'jks',material:fs.readFileSync('node_modules/jks-js/examples/assets/truststore.jks').toString('base64'),password:'password'});assert.equal(trust.ok,true);
        const decoded=await run('record-codec-worker',{input:{format:'json',bytes:Buffer.from('{"nativeImage":true}').toString('base64')},bundle:null});assert.equal(decoded.state,'decoded');assert.equal(JSON.parse(decoded.json).nativeImage,true);
        console.log('native-workers-qualified');
      })().catch(()=>{process.exitCode=1;});
    `,
  );
  assert.equal(worker, "native-workers-qualified");
}
