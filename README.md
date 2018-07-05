# FTP file input plugin for Embulk

## Overview

This plugin support **FTP**, **FTPES(FTPS explicit)**, **FTPS(FTPS implicit)** and doesn't support **SFTP**.

If you want to use SFTP, please use [embulk-input-sftp](https://github.com/embulk/embulk-input-sftp).


* **Plugin type**: file input
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **host**: FTP server address (string, required)
- **port**: FTP server port number (integer, default: `21`. `990` if `ssl` is true and `ssl_explicit` is false)
- **user**: user name to login (string, optional)
- **password**: password to login (string, default: `""`)
- **path_prefix** prefix of target files (string, required)
- **incremental** enables incremental loading(boolean, optional. default: true. If incremental loading is enabled, config diff for the next execution will include last_path parameter so that next execution skips files before the path. Otherwise, last_path will not be included.
- **passive_mode**: use passive mode (boolean, default: true)
- **ascii_mode**: use ASCII mode instead of binary mode (boolean, default: false)
- **ssl**: use FTPS (SSL encryption). (boolean, default: false)
- **ssl_explicit** use FTPS(explicit) instead of FTPS(implicit). (boolean, default:true)
- **ssl_verify**: verify the certification provided by the server. By default, connection fails if the server certification is not signed by one the CAs in JVM's default trusted CA list. (boolean, default: true)
- **ssl_verify_hostname**: verify server's hostname matches with provided certificate. (boolean, default: true)
- **ssl_trusted_ca_cert_file**: if the server certification is not signed by a certificate authority, set path to the X.508 certification file (pem file) of a private CA (string, optional)
- **ssl_trusted_ca_cert_data**: similar to `ssl_trusted_ca_cert_file` but embed the contents of the PEM file as a string value instead of path to a local file (string, optional)

### FTP / FTPS default port number

FTP and FTPS server usually listens following port number(TCP) as default.

Please be sure to configure firewall rules.

|                         | FTP | FTPS(explicit) = FTPES | FTPS(implicit) = FTPS |
|:------------------------|----:|-----------------------:|----------------------:|
| Control channel port    |  21 |                     21 |             990 (\*1) |
| Data channel port (\*2) |  20 |                     20 |                   989 |

1. If you're using both of FTPS(implicit) and FTP, server may also listen 21/TCP for unecnrypted FTP.
2. If you're using `passive mode`, data channel port can be taken between 1024 and 65535.

## Example

Simple FTP:

```yaml
in:
  type: ftp
  host: ftp.example.net
  port: 21
  user: anonymous
  path_prefix: /ftp/file/path/prefix
```

FTPS encryption without server certificate verification:

```yaml
in:
  type: ftp
  host: ftp.example.net
  port: 21
  user: anonymous
  password: "mypassword"
  path_prefix: /ftp/file/path/prefix

  ssl: true
  ssl_verify: false
```

FTPS encryption with server certificate verification:

```yaml
in:
  type: ftp
  host: ftp.example.net
  port: 21
  user: anonymous
  password: "mypassword"
  path_prefix: /ftp/file/path/prefix

  ssl: true
  ssl_verify: true

  ssl_verify_hostname: false   # to disable server hostname verification (optional)

  # if the server use self-signed certificate, or set path to the pem file (optional)
  ssl_trusted_ca_cert_file: /path/to/ca_cert.pem

  # or embed contents of the pem file here (optional)
  ssl_trusted_ca_cert_data: |
      -----BEGIN CERTIFICATE-----
      MIIFV...
      ...
      ...
      -----END CERTIFICATE-----
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
$ ./gradlew bintrayUpload # release embulk-input-sftp to Bintray maven repo
```

## Test

```
$ ./gradlew test  # -t to watch change of files and rebuild continuously
```
To run unit tests, we need to configure the following environment variables.

When environment variables are not set, skip some test cases.

```
FTP_TEST_HOST
FTP_TEST_USER
FTP_TEST_PASSWORD
FTP_TEST_SSL_TRUSTED_CA_CERT_FILE
FTP_TEST_SSL_TRUSTED_CA_CERT_DATA
```

Following option is optional
```
FTP_TEST_PORT (default:21)
FTP_TEST_SSL__PORT (default:990)
```

If you're using Mac OS X El Capitan and GUI Applications(IDE), like as follows.
```xml

launchctl setenv FTP_TEST_SSL_TRUSTED_CA_CERT_FILE$ vi ~/Library/LaunchAgents/environment.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>my.startup</string>
  <key>ProgramArguments</key>
  <array>
    <string>sh</string>
    <string>-c</string>
    <string>
      launchctl setenv FTP_TEST_HOST ftp.example.com
      launchctl setenv FTP_TEST_USER username
      launchctl setenv FTP_TEST_PASSWORD password
      launchctl setenv FTP_TEST_SSL_TRUSTED_CA_CERT_FILE /path/to/cert.pem
      launchctl setenv FTP_TEST_SSL_TRUSTED_CA_CERT_DATA "-----BEGIN CERTIFICATE-----
      ABCDEFG...
      EFGHIJKL...
      -----END CERTIFICATE-----"
    </string>
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>

$ launchctl load ~/Library/LaunchAgents/environment.plist
$ launchctl getenv FTP_TEST_HOST //try to get value.

Then start your applications.
```