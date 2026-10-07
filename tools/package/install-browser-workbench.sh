#!/usr/bin/env bash
# Release CI stamps this installer; installation never builds or upgrades source.
set +x
set -Eeuo pipefail
umask 077

STREAMSKOPE_INSTALL_VERSION='@STREAMSKOPE_INSTALL_VERSION@'
STREAMSKOPE_INSTALL_SOURCE='@STREAMSKOPE_INSTALL_SOURCE@'
STREAMSKOPE_TOPOLOGY_SHA256='@STREAMSKOPE_TOPOLOGY_SHA256@'
STREAMSKOPE_MANIFEST_SHA256='@STREAMSKOPE_MANIFEST_SHA256@'
INSTALL_ROOT='/var/lib/streamskope/browser'
STATE_ROOT=$INSTALL_ROOT
LAB_NAME='streamskope'
CONTAINER_NAME='clab-streamskope-app'
STATE_OWNER_UID=0
STATE_OWNER_GID=0
FIRST_PORT=8080
OS_RELEASE='/etc/os-release'
TTY_DEVICE='/dev/tty'
APT_KEYRING='/etc/apt/keyrings/streamskope-docker.asc'
DOCKER_APT_SOURCE='/etc/apt/sources.list.d/streamskope-docker.sources'
CLAB_APT_SOURCE='/etc/apt/sources.list.d/streamskope-containerlab.list'
STAGING=''

fail() { printf 'StreamSkope: %s\n' "$*" >&2; exit 1; }
log() { printf 'StreamSkope: %s\n' "$*" >&2; }
cleanup() { if [[ -n "$STAGING" ]]; then rm -rf -- "$STAGING"; fi; }
trap cleanup EXIT
trap 'fail "Installation stopped. Saved deployment and vault data were preserved; resolve the reported problem and run the installer again."' ERR

[[ $# -eq 0 ]] || fail 'This installer accepts no arguments.'
[[ "$STREAMSKOPE_INSTALL_VERSION" != @* && "$STREAMSKOPE_INSTALL_SOURCE" != @* && "$STREAMSKOPE_TOPOLOGY_SHA256" != @* && "$STREAMSKOPE_MANIFEST_SHA256" != @* ]] || fail 'Download the stamped installer from a published StreamSkope release; the source template cannot install.'
[[ "$STREAMSKOPE_INSTALL_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ && "$STREAMSKOPE_INSTALL_SOURCE" =~ ^[a-f0-9]{40}$ && "$STREAMSKOPE_TOPOLOGY_SHA256" =~ ^[a-f0-9]{64}$ && "$STREAMSKOPE_MANIFEST_SHA256" =~ ^[a-f0-9]{64}$ ]] || fail 'Installer release identity is invalid.'
[[ $(id -u) -eq 0 ]] || fail 'Run this installer with sudo from the account that will own the workbench.'
[[ $(uname -s) == Linux ]] || fail 'Install inside a Linux Docker host or Linux VM.'
case $(uname -m) in x86_64|amd64) ARCHITECTURE=amd64 ;; aarch64|arm64) ARCHITECTURE=arm64 ;; *) fail 'The browser image requires Linux AMD64 or ARM64.' ;; esac
[[ -z ${DOCKER_CONTEXT:-} || ${DOCKER_CONTEXT:-} == default ]] || fail 'Select the local default Docker context before installing; remote contexts are not supported.'
[[ -z ${DOCKER_HOST:-} || ${DOCKER_HOST:-} == unix:///var/run/docker.sock || ${DOCKER_HOST:-} == unix:///run/docker.sock ]] || fail 'Use the local Linux Docker daemon; remote Docker endpoints are not supported.'

# Docker repositories: https://docs.docker.com/engine/install/{ubuntu,debian}/
# Containerlab repository: https://containerlab.dev/install/#package-managers

prerequisite_package_installed() {
  [[ "$(dpkg-query -W -f='${db:Status-Abbrev}' "$1" 2>/dev/null || true)" == ii* ]]
}

prerequisite_existing_repository_guard() {
  local expression=$1 managed=$2 file
  for file in /etc/apt/sources.list "$(dirname -- "$managed")/"*.list "$(dirname -- "$managed")/"*.sources; do
    [[ "$file" == "$managed" ]] && continue
    [[ -f "$file" ]] || continue
    if grep -Eq "$expression" "$file"; then
      fail "An existing prerequisite repository is configured in $file. Install the missing tool through that repository, then retry; StreamSkope will not replace it or add a conflicting repository."
    fi
  done
}

prerequisite_guard_destination() {
  local destination="$1" current="$1" permissions owner
  while [[ ! -e "$current" && ! -L "$current" ]]; do
    current="$(dirname -- "$current")"
  done
  [[ ! -L "$current" ]] || fail "Prerequisite path is a symbolic link: $current"
  owner="$(stat -c '%u:%g' -- "$current")"
  [[ "$owner" == "$STATE_OWNER_UID:$STATE_OWNER_GID" ]] ||
    fail "Prerequisite path has an unexpected owner: $current"
  permissions="$(stat -c '%a' -- "$current")"
  (( (8#$permissions & 8#022) == 0 )) ||
    fail "Prerequisite path is writable by another user: $current"
  if [[ "$current" == "$destination" ]]; then
    [[ -f "$destination" ]] || fail "Prerequisite destination must be a regular file: $destination"
  else
    [[ -d "$current" ]] || fail "Prerequisite parent must be a directory: $current"
  fi
  # Check existing direct parents as well; never follow a substituted repository directory.
  current="$(dirname -- "$destination")"
  while [[ "$current" != / && ( -e "$current" || -L "$current" ) ]]; do
    [[ -d "$current" && ! -L "$current" ]] || fail "Unsafe prerequisite directory: $current"
    owner="$(stat -c '%u:%g' -- "$current")"
    permissions="$(stat -c '%a' -- "$current")"
    if [[ "$owner" != "$STATE_OWNER_UID:$STATE_OWNER_GID" ]] || (( (8#$permissions & 8#022) != 0 )); then
      fail "Unsafe prerequisite directory: $current"
    fi
    current="$(dirname -- "$current")"
  done
}

# Read-only preflight; call before creating installation state or doing any apt writes.
check_prerequisites() {
  local key value package conflicts=()
  INSTALL_NEED_DOCKER=0 INSTALL_NEED_CLAB=0 INSTALL_NEED_BOOTSTRAP=0
  INSTALL_OS_ID='' INSTALL_OS_VERSION='' INSTALL_OS_CODENAME=''
  command -v docker >/dev/null 2>&1 || INSTALL_NEED_DOCKER=1
  if ! command -v containerlab >/dev/null 2>&1 && ! command -v clab >/dev/null 2>&1; then
    INSTALL_NEED_CLAB=1
  fi
  for value in curl python3 flock; do
    command -v "$value" >/dev/null 2>&1 || INSTALL_NEED_BOOTSTRAP=1
  done
  if (( INSTALL_NEED_DOCKER + INSTALL_NEED_CLAB + INSTALL_NEED_BOOTSTRAP == 0 )); then
    return
  fi
  [[ -r "$OS_RELEASE" ]] ||
    fail 'Install the missing prerequisites manually; this host has no supported distribution identity.'
  while IFS='=' read -r key value; do
    value="${value#\"}"; value="${value%\"}"
    value="${value#\'}"; value="${value%\'}"
    case "$key" in
      ID) INSTALL_OS_ID="$value" ;;
      VERSION_ID) INSTALL_OS_VERSION="$value" ;;
    esac
  done < "$OS_RELEASE"
  case "$INSTALL_OS_ID:$INSTALL_OS_VERSION" in
    ubuntu:22.04) INSTALL_OS_CODENAME=jammy ;;
    ubuntu:24.04) INSTALL_OS_CODENAME=noble ;;
    debian:12) INSTALL_OS_CODENAME=bookworm ;;
    debian:13) INSTALL_OS_CODENAME=trixie ;;
    *) fail 'Automatic prerequisite installation supports Ubuntu 22.04/24.04 and Debian 12/13. Install Docker, Containerlab, curl, Python 3 and flock manually on this host, then retry.' ;;
  esac
  if ! command -v apt-get >/dev/null 2>&1 || ! command -v dpkg-query >/dev/null 2>&1 || ! command -v dpkg >/dev/null 2>&1; then
    fail 'The supported distribution needs its standard apt and dpkg tools.'
  fi
  INSTALL_APT_ARCH="$(dpkg --print-architecture)"
  case "$INSTALL_APT_ARCH" in amd64|arm64) ;; *) fail 'Browser installation supports AMD64 and ARM64 Linux hosts.' ;; esac
  if (( INSTALL_NEED_DOCKER )); then
    for package in docker.io docker-compose docker-compose-v2 docker-doc docker-buildx podman-docker containerd runc; do
      prerequisite_package_installed "$package" && conflicts+=("$package")
    done
    for package in dockerd containerd runc; do
      command -v "$package" >/dev/null 2>&1 && conflicts+=("$package binary")
    done
    (( ${#conflicts[@]} == 0 )) ||
      fail "Existing runtime packages (${conflicts[*]}) require a manually prepared Docker installation. StreamSkope will not remove or replace them; use a separate Linux host or prepare Docker without disrupting existing workloads, then retry."
    command -v systemctl >/dev/null 2>&1 || command -v service >/dev/null 2>&1 ||
      fail 'Prepare a running local Docker daemon manually on this host, then retry.'
    prerequisite_existing_repository_guard '^[[:space:]]*(deb[[:space:]]|URIs:[[:space:]]).*https?://download\.docker\.com/linux/' "$DOCKER_APT_SOURCE"
    prerequisite_guard_destination "$APT_KEYRING"
    prerequisite_guard_destination "$DOCKER_APT_SOURCE"
  fi
  if (( INSTALL_NEED_CLAB )); then
    prerequisite_existing_repository_guard '^[[:space:]]*(deb[[:space:]]|URIs:[[:space:]]).*https?://netdevops\.fury\.site/apt/' "$CLAB_APT_SOURCE"
    prerequisite_guard_destination "$CLAB_APT_SOURCE"
  fi
}

install_missing_prerequisites() (
  local temporary packages=()
  (( INSTALL_NEED_DOCKER + INSTALL_NEED_CLAB + INSTALL_NEED_BOOTSTRAP > 0 )) || return 0
  log 'Installing missing host prerequisites from their package repositories.'
  apt-get update
  for package in ca-certificates curl python3 util-linux; do
    prerequisite_package_installed "$package" || packages+=("$package")
  done
  if (( ${#packages[@]} )); then
    DEBIAN_FRONTEND=noninteractive apt-get --no-remove --no-upgrade install -y --no-install-recommends "${packages[@]}"
  fi
  temporary="$(mktemp -d)"
  trap 'rm -rf -- "$temporary"' EXIT
  if (( INSTALL_NEED_DOCKER )); then
    install -d -m 0755 -- "$(dirname -- "$APT_KEYRING")" "$(dirname -- "$DOCKER_APT_SOURCE")"
    curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
      --connect-timeout 15 --max-time 120 \
      "https://download.docker.com/linux/$INSTALL_OS_ID/gpg" -o "$temporary/docker.asc"
    if [[ -e "$APT_KEYRING" ]]; then
      cmp -s -- "$temporary/docker.asc" "$APT_KEYRING" || fail 'The existing StreamSkope Docker key differs from the official download; review it manually before retrying.'
    else
      install -m 0644 -- "$temporary/docker.asc" "$APT_KEYRING"
    fi
    printf 'Types: deb\nURIs: https://download.docker.com/linux/%s\nSuites: %s\nComponents: stable\nArchitectures: %s\nSigned-By: %s\n' \
      "$INSTALL_OS_ID" "$INSTALL_OS_CODENAME" "$INSTALL_APT_ARCH" "$APT_KEYRING" > "$temporary/docker.sources"
    if [[ -e "$DOCKER_APT_SOURCE" ]]; then
      cmp -s -- "$temporary/docker.sources" "$DOCKER_APT_SOURCE" || fail 'The existing StreamSkope Docker apt source differs; review it manually before retrying.'
    else
      install -m 0644 -- "$temporary/docker.sources" "$DOCKER_APT_SOURCE"
    fi
  fi
  if (( INSTALL_NEED_CLAB )); then
    install -d -m 0755 -- "$(dirname -- "$CLAB_APT_SOURCE")"
    # This is the official HTTPS package repository. It uses trusted=yes and has
    # no repository signing key; do not describe it as signed package delivery.
    printf 'deb [trusted=yes] https://netdevops.fury.site/apt/ /\n' > "$temporary/containerlab.list"
    if [[ -e "$CLAB_APT_SOURCE" ]]; then
      cmp -s -- "$temporary/containerlab.list" "$CLAB_APT_SOURCE" || fail 'The existing StreamSkope Containerlab apt source differs; review it manually before retrying.'
    else
      install -m 0644 -- "$temporary/containerlab.list" "$CLAB_APT_SOURCE"
    fi
  fi
  apt-get update
  packages=()
  (( INSTALL_NEED_DOCKER )) && packages+=(docker-ce docker-ce-cli containerd.io)
  (( INSTALL_NEED_CLAB )) && packages+=(containerlab)
  if (( ${#packages[@]} )); then
    DEBIAN_FRONTEND=noninteractive apt-get --no-remove --no-upgrade install -y --no-install-recommends "${packages[@]}"
  fi
  if (( INSTALL_NEED_DOCKER )); then
    if command -v systemctl >/dev/null 2>&1; then systemctl enable --now docker;
    else service docker start; fi
  fi
  for package in curl python3 flock docker; do
    command -v "$package" >/dev/null 2>&1 || fail "Prerequisite installation did not provide $package."
  done
  command -v containerlab >/dev/null 2>&1 || command -v clab >/dev/null 2>&1 ||
    fail 'Prerequisite installation did not provide Containerlab.'
)

check_prerequisites
install_missing_prerequisites >&2

safe_directory() {
  local path=$1 owner=$2
  [[ -d "$path" && ! -L "$path" ]] || fail "A regular directory is required at $path; no existing paths were replaced."
  [[ $(stat -c '%u' "$path") == "$owner" ]] || fail "Directory ownership does not match at $path; no existing data was adopted."
  local permissions
  permissions=$(stat -c '%a' "$path")
  if [[ ! "$permissions" =~ ^[0-7]{3,4}$ ]] || (( (8#$permissions & 0022) != 0 )); then
    fail "Remove group/other write access from $path before retrying."
  fi
}
safe_file() {
  [[ -f "$1" && ! -L "$1" && $(stat -c '%u:%a:%h' "$1") == "$STATE_OWNER_UID:600:1" ]] || fail "A private, singly linked deployment file is required at $1; no file was replaced."
}
parent=$(dirname "$STATE_ROOT")
if [[ -e "$parent" || -L "$parent" ]]; then safe_directory "$parent" "$STATE_OWNER_UID"; else install -d -m 0755 -o "$STATE_OWNER_UID" -g "$STATE_OWNER_GID" "$parent"; fi
if [[ -e "$STATE_ROOT" || -L "$STATE_ROOT" ]]; then safe_directory "$STATE_ROOT" "$STATE_OWNER_UID"; [[ $(stat -c '%a' "$STATE_ROOT") == 700 ]] || fail 'Deployment state must have mode 0700.'; else install -d -m 0700 -o "$STATE_OWNER_UID" -g "$STATE_OWNER_GID" "$STATE_ROOT"; fi
lock="$STATE_ROOT/installer.lock"
if [[ -e "$lock" || -L "$lock" ]]; then safe_file "$lock"; fi
exec 9>"$lock"
flock --nonblock 9 || fail 'Another StreamSkope installer is running. Wait for it to finish and retry.'
STAGING=$(mktemp -d "$STATE_ROOT/.install-XXXXXXXX")
state="$STATE_ROOT/installation.json"
data="$STATE_ROOT/streamskope-data"
saved=false
version=$STREAMSKOPE_INSTALL_VERSION
source=$STREAMSKOPE_INSTALL_SOURCE
topology_hash=$STREAMSKOPE_TOPOLOGY_SHA256
manifest_hash=$STREAMSKOPE_MANIFEST_SHA256
if [[ -e "$state" || -L "$state" ]]; then
  safe_file "$state"
  values=$(python3 - "$state" <<'PY'
import json,re,sys
try:
    value=json.load(open(sys.argv[1],encoding='utf8'))
    fields=['schemaVersion','version','sourceRevision','topologySha256','manifestSha256','uid','gid','home','operatorUid','port']
    assert isinstance(value,dict) and set(value)==set(fields) and type(value['schemaVersion']) is int and value['schemaVersion']==1
    assert isinstance(value['version'],str) and re.fullmatch(r'(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?',value['version'])
    for field,size in [('sourceRevision',40),('topologySha256',64),('manifestSha256',64)]:
        assert isinstance(value[field],str) and re.fullmatch('[a-f0-9]{%d}'%size,value[field])
    assert all(type(value[field]) is int and value[field]>=0 for field in ['uid','gid','operatorUid']) and value['uid']>0
    assert type(value['port']) is int and 1024<=value['port']<=65535
    assert isinstance(value['home'],str) and value['home'].startswith('/') and not any(c in value['home'] for c in '\r\n\0')
    for field in fields[1:]: print(value[field])
except (AssertionError,ValueError,OSError,TypeError,KeyError):
    sys.exit('Saved installation metadata is invalid; preserve it and recover the trusted deployment record before retrying.')
PY
  )
  mapfile -t pinned <<<"$values"
  version=${pinned[0]}; source=${pinned[1]}; topology_hash=${pinned[2]}; manifest_hash=${pinned[3]}
  saved_uid=${pinned[4]}; saved_gid=${pinned[5]}; saved_home=${pinned[6]}; saved_operator=${pinned[7]}; port=${pinned[8]}
  saved=true
  [[ "$version" == "$STREAMSKOPE_INSTALL_VERSION" ]] || log "Reusing installed v$version; rerunning an installer does not upgrade an existing deployment."
fi

endpoint=$(docker context inspect --format '{{json .Endpoints.docker.Host}}')
[[ "$endpoint" == '"unix:///var/run/docker.sock"' || "$endpoint" == '"unix:///run/docker.sock"' ]] || fail 'The selected Docker context is not a local Linux daemon.'
export DOCKER_HOST=unix:///var/run/docker.sock
export DOCKER_CONTEXT=default
docker_local() { docker --host unix:///var/run/docker.sock "$@"; }
docker_local info --format '{{json .}}' >"$STAGING/daemon.json" || fail 'The local Docker daemon is unavailable. Start it, then retry; the installer will not replace an existing Docker installation.'
python3 - "$STAGING/daemon.json" <<'PY'
import json,sys
value=json.load(open(sys.argv[1],encoding='utf8'))
if not isinstance(value,dict) or value.get('OSType')!='linux': sys.exit('Docker must run a local Linux daemon.')
PY
container_id=$(docker_local container ls --all --filter "name=^/$CONTAINER_NAME$" --format '{{.ID}}')
[[ -z "$container_id" || "$saved" == true ]] || fail "An existing $CONTAINER_NAME is unrelated to this installer. Keep it and use its existing deployment procedure."

operator_uid=${SUDO_UID:-0}
if [[ "$operator_uid" != 0 ]]; then
  [[ "$operator_uid" =~ ^[1-9][0-9]*$ && ${SUDO_GID:-} =~ ^[0-9]+$ ]] || fail 'The original sudo account identity is invalid.'
  account=$(getent passwd "$operator_uid") || fail 'The original sudo account no longer exists.'
else
  account=$(getent passwd streamskope-browser || true)
  if [[ -z "$account" ]]; then
    useradd --system --user-group --home-dir "$STATE_ROOT" --no-create-home --shell /usr/sbin/nologin streamskope-browser
    account=$(getent passwd streamskope-browser)
  fi
fi
IFS=: read -r owner_name _ owner_uid owner_gid _ owner_home owner_shell <<<"$account"
[[ "$owner_uid" =~ ^[1-9][0-9]*$ && "$owner_gid" =~ ^[0-9]+$ && "$owner_home" == /* ]] || fail 'A non-root numeric data owner and an absolute account home are required.'
if [[ "$operator_uid" != 0 ]]; then
  [[ "$owner_uid" == "$operator_uid" && "$owner_gid" == "$SUDO_GID" ]] || fail 'The original sudo UID/GID does not match its account.'
else
  [[ "$owner_name" == streamskope-browser && "$owner_home" == "$STATE_ROOT" && "$owner_shell" == /usr/sbin/nologin ]] || fail 'The streamskope-browser account belongs to another deployment; it was not changed.'
fi
if [[ "$saved" == true ]]; then
  [[ "$owner_uid:$owner_gid:$owner_home:$operator_uid" == "$saved_uid:$saved_gid:$saved_home:$saved_operator" ]] || fail 'This deployment belongs to another account. Run the installer through its original owner; data was preserved.'
fi

topology="streamskope-$version.clab.yml"
manifest="streamskope-$version-container.json"
base="https://github.com/asadarafat/streamskope/releases/download/v$version"
for file in SHA256SUMS "$topology" "$manifest"; do
  if [[ -e "$STATE_ROOT/$file" || -L "$STATE_ROOT/$file" ]]; then safe_file "$STATE_ROOT/$file"; cp -- "$STATE_ROOT/$file" "$STAGING/$file";
  else curl --proto '=https' --proto-redir '=https' --tlsv1.2 --fail --location --silent --show-error --connect-timeout 15 --max-time 120 --max-filesize 1048576 --output "$STAGING/$file" "$base/$file" || fail "Cannot retrieve v$version release assets. Check HTTPS/proxy access to GitHub, then retry."; fi
done
printf '%s  %s\n%s  %s\n' "$topology_hash" "$topology" "$manifest_hash" "$manifest" >"$STAGING/expected.sha256"
(cd "$STAGING" && sha256sum --check --status expected.sha256) || fail 'Release metadata hashes do not match the stamped installer; no deployment was changed.'
release=$(python3 - "$STAGING" "$version" "$source" "$topology_hash" "$manifest_hash" "$ARCHITECTURE" <<'PY'
import json,re,sys
from pathlib import Path
root=Path(sys.argv[1]); version,source,topology_hash,manifest_hash,arch=sys.argv[2:]
try:
    topology='streamskope-'+version+'.clab.yml'; manifest='streamskope-'+version+'-container.json'
    checks={}
    for line in (root/'SHA256SUMS').read_text().splitlines():
        match=re.fullmatch(r'([a-f0-9]{64}) [ *]([^/\\\0]+)',line)
        assert match and match[2] not in checks
        checks[match[2]]=match[1]
    assert checks.get(topology)==topology_hash and checks.get(manifest)==manifest_hash
    value=json.loads((root/manifest).read_text()); registry=value['registry']
    assert value['schemaVersion']==2 and value['version']==version and value['sourceRevision']==source
    assert value['format']=='docker-save-gzip' and value['image']=='streamskope:'+version
    assert value['topology']=={'file':topology,'sha256':topology_hash}
    assert registry['schemaVersion']==1 and registry['version']==version and registry['sourceRevision']==source
    image='ghcr.io/asadarafat/streamskope:'+version; digest=registry['digest']
    assert re.fullmatch(r'sha256:[a-f0-9]{64}',digest) and registry['image']==image and registry['reference']==image+'@'+digest
    platforms=registry['platforms']; assert [p['platform'] for p in platforms]==['linux/amd64','linux/arm64']
    for platform in platforms:
        assert re.fullmatch(r'sha256:[a-f0-9]{64}',platform['manifestDigest']) and re.fullmatch(r'sha256:[a-f0-9]{64}',platform['imageId'])
    assert platforms[0]['manifestDigest']!=platforms[1]['manifestDigest'] and platforms[0]['imageId']!=platforms[1]['imageId']
    text=(root/topology).read_text()
    for expected in ['name: streamskope','image: ${STREAMSKOPE_IMAGE:='+registry['reference']+'}','image-pull-policy: IfNotPresent','./streamskope-data:/data','${STREAMSKOPE_UID:=1000}:${STREAMSKOPE_GID:=1000}','${STREAMSKOPE_HOST_BIND:=127.0.0.1}:${STREAMSKOPE_HOST_PORT:=8080}:8080/tcp','${STREAMSKOPE_PUBLIC_ORIGIN:=http://127.0.0.1:8080}']:
        assert text.count(expected)==1
    print(registry['reference']); print(next(p['imageId'] for p in platforms if p['platform']=='linux/'+arch))
except (AssertionError,ValueError,OSError,TypeError,KeyError,StopIteration):
    sys.exit('Published browser metadata, topology or SHA256SUMS does not match this release; installation was stopped.')
PY
)
mapfile -t image_details <<<"$release"
image=${image_details[0]}; image_id=${image_details[1]}
if [[ -e "$data" || -L "$data" ]]; then
  safe_directory "$data" "$owner_uid"
  [[ $(stat -c '%a' "$data") == 700 && $(stat -c '%g' "$data") == "$owner_gid" ]] || fail 'Existing data ownership or private permissions do not match; data was preserved.'
  [[ "$saved" == true || -z $(find "$data" -mindepth 1 -maxdepth 1 -print -quit) ]] || fail 'Data exists without a trusted installation record; it was not adopted.'
else
  [[ "$saved" == false ]] || fail 'Saved deployment data is missing; restore it before retrying instead of creating a new vault.'
  install -d -m 0700 -o "$owner_uid" -g "$owner_gid" "$data"
fi
if [[ "$saved" == false ]]; then
  port=$(python3 - "$FIRST_PORT" <<'PY'
import socket,sys
for port in range(int(sys.argv[1]),65536):
    try:
        with socket.socket() as listener: listener.bind(('127.0.0.1',port))
        print(port); break
    except OSError: continue
else: sys.exit('No local browser port is available.')
PY
  )
fi
origin="http://127.0.0.1:$port"
for file in SHA256SUMS "$topology" "$manifest"; do install -m 0600 -o "$STATE_OWNER_UID" -g "$STATE_OWNER_GID" "$STAGING/$file" "$STATE_ROOT/$file"; done
if [[ "$saved" == false ]]; then
  python3 - "$state" "$version" "$source" "$topology_hash" "$manifest_hash" "$owner_uid" "$owner_gid" "$owner_home" "$operator_uid" "$port" <<'PY'
import json,os,sys,tempfile
path,version,source,topology,manifest,uid,gid,home,operator,port=sys.argv[1:]
value={'schemaVersion':1,'version':version,'sourceRevision':source,'topologySha256':topology,'manifestSha256':manifest,'uid':int(uid),'gid':int(gid),'home':home,'operatorUid':int(operator),'port':int(port)}
with tempfile.NamedTemporaryFile(mode='w',dir=os.path.dirname(path),prefix='.installation-',delete=False) as output:
    json.dump(value,output); output.write('\n'); output.flush(); os.fsync(output.fileno()); temporary=output.name
os.replace(temporary,path)
PY
fi
export STREAMSKOPE_IMAGE="$image" STREAMSKOPE_UID="$owner_uid" STREAMSKOPE_GID="$owner_gid"
export STREAMSKOPE_HOST_BIND=127.0.0.1 STREAMSKOPE_HOST_PORT="$port" STREAMSKOPE_PUBLIC_ORIGIN="$origin"

inspect_owned_container() {
  docker_local inspect "$CONTAINER_NAME" >"$STAGING/container.json"
  python3 - "$STAGING/container.json" "$CONTAINER_NAME" "$LAB_NAME" "$STATE_ROOT/$topology" "$data" "$image" "$image_id" "$owner_uid:$owner_gid" "$port" "$origin" <<'PY'
import json,sys
try:
    path,name,lab,topology,data,image,identity,user,port,origin=sys.argv[1:]; value=json.load(open(path))[0]
    config=value['Config']; labels=config['Labels']; host=value['HostConfig']
    assert value['Name']=='/'+name and config['Image']==image and value['Image']==identity and config['User']==user
    assert labels['containerlab']==lab and labels['clab-topo-file']==topology and labels['io.streamskope.deployment']=='browser'
    assert host['Privileged'] is False and host['PortBindings'].get('8080/tcp')==[{'HostIp':'127.0.0.1','HostPort':port}]
    assert 'STREAMSKOPE_PUBLIC_ORIGIN='+origin in config['Env']
    mounts=[m for m in value['Mounts'] if m['Destination']=='/data']
    assert len(mounts)==1 and mounts[0]['Type']=='bind' and mounts[0]['Source']==data and mounts[0]['RW'] is True
    print('running' if value['State']['Running'] else 'stopped')
except (AssertionError,ValueError,OSError,TypeError,KeyError,IndexError):
    sys.exit('The existing container does not belong to this pinned deployment; it was not changed.')
PY
}
if [[ -n "$container_id" ]]; then
  running=$(inspect_owned_container)
  [[ "$running" == running ]] || docker_local start "$CONTAINER_NAME" >/dev/null
else
  python3 - "$port" <<'PY'
import socket,sys
try:
    with socket.socket() as listener: listener.bind(('127.0.0.1',int(sys.argv[1])))
except OSError: sys.exit('The saved browser port is now occupied; release that port and retry. Deployment data was preserved.')
PY
  if ! docker_local image inspect "$image" >"$STAGING/image.json" 2>/dev/null; then docker_local pull "$image" >&2; docker_local image inspect "$image" >"$STAGING/image.json"; fi
  python3 - "$STAGING/image.json" "$image_id" "$version" "$source" "$ARCHITECTURE" <<'PY'
import json,sys
path,identity,version,source,arch=sys.argv[1:]; value=json.load(open(path))[0]; labels=value['Config']['Labels']
if value.get('Id')!=identity or value.get('Os')!='linux' or value.get('Architecture')!=arch or labels.get('org.opencontainers.image.version')!=version or labels.get('org.opencontainers.image.revision')!=source:
    sys.exit('The local image does not match the qualified release and architecture; no container was deployed.')
PY
  if command -v containerlab >/dev/null; then clab=containerlab; else clab=clab; fi
  (cd "$STATE_ROOT" && "$clab" --runtime docker deploy --topo "$STATE_ROOT/$topology" --name "$LAB_NAME" --max-workers 1) >&2 || fail 'Containerlab could not deploy. Pinned state and private data remain available for a retry; do not remove vault data.'
fi
[[ $(inspect_owned_container) == running ]] || fail 'The pinned container is not running; inspect its health and retry without removing data.'

ready=false
for _ in {1..60}; do
  if curl --fail --silent --connect-timeout 2 --max-time 3 --output "$STAGING/health.json" "$origin/health" && curl --fail --silent --connect-timeout 2 --max-time 3 --output "$STAGING/session.json" "$origin/__streamskope_session/status"; then
    if python3 - "$STAGING/health.json" "$STAGING/session.json" <<'PY'
import json,sys
try:
    health=json.load(open(sys.argv[1])); session=json.load(open(sys.argv[2]))
    assert health.get('status') in ['locked','ready'] and session.get('state')=='locked' and type(session.get('setupRequired')) is bool
except (AssertionError,ValueError,OSError,TypeError): sys.exit(1)
PY
    then ready=true; break; fi
  fi
  sleep 1
done
[[ "$ready" == true ]] || fail 'The pinned browser did not become ready at its configured origin. Inspect the container health and retry; private data was preserved.'
setup=$(python3 - "$STAGING/session.json" <<'PY'
import json,sys
print('true' if json.load(open(sys.argv[1]))['setupRequired'] else 'false')
PY
)
if [[ "$setup" == true ]]; then
  python3 - "$data/setup-code" "$owner_uid" "$operator_uid" "$TTY_DEVICE" <<'PY'
import os,re,stat,sys
path,owner,operator,terminal=sys.argv[1:]; descriptor=None
try:
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW); metadata=os.fstat(descriptor)
    assert stat.S_ISREG(metadata.st_mode) and metadata.st_uid==int(owner) and metadata.st_nlink==1 and stat.S_IMODE(metadata.st_mode)==0o600
    code=os.read(descriptor,129).decode().strip(); assert re.fullmatch(r'[A-Za-z0-9_-]{43}',code)
    target=None
    if os.isatty(1) and os.fstat(1).st_uid==int(operator): target=1
    else:
        try:
            target=os.open(terminal,os.O_WRONLY|os.O_NOCTTY)
            if not os.isatty(target): os.close(target); target=None
        except OSError: pass
    if target is not None:
        os.write(target,('Setup code: '+code+'\n').encode())
        if target!=1: os.close(target)
except (OSError,AssertionError,UnicodeError): sys.exit('The private setup-code file is unavailable or unsafe; inspect its ownership before creating the vault.')
finally:
    if descriptor is not None: os.close(descriptor)
PY
  printf 'Open %s and select Create vault. The setup code is shown only in your terminal; rerun interactively if it was not shown.\n' "$origin"
else
  printf 'Open %s and select Unlock with your existing vault passphrase.\n' "$origin"
fi
