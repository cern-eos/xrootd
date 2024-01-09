# XRDS3

## Configuration

- Add the following fields to the /etc/xrootd.conf file:
  - `http.exthandler xrds3 libXrdHttpS3.so`
  - `s3.dataddir`
  - `s3.authdir`
  - `s3.region`
  - `s3.service`

Example configuration file:
```
xrd.protocol XrdHttp:8080 libXrdHttp.so
xrd.tls /etc/grid-security/daemon/hostcert.pem /etc/grid-security/daemon/hostkey.pem detail
xrd.tlsca certdir /etc/grid-security/certificates/
http.gridmap /etc/grid-security/grid-mapfile
http.trace all
http.exthandler xrds3 libXrdHttpS3.so

s3.datadir /xrootd/data
s3.authdir /xrootd/auth
s3.region us-east-1
s3.service s3
```

The two data and auth directories need to exist.

- Create a file named `users` in the authdir.
It will need to contain pairs of access key id and secret access key, separated by a `:`

Example with 3 credentials:
```
AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
NOPQRSTUVWXYZABCDEFG:nopqrstuvwxyzabcdefghijklmnabcdefghijklm
HIJKLMNOPQRSTUVWXYZA:opqrstuvwxyzabcdefghijklmnopqrstuvwxyzab
```

## Usage

The s3 service will run on the same port as the xrootd http service.

### AWS Cli v2

- Run `aws configure` and fill in the requested values

Every `aws` command needs to have the `--endpoint-url <url>` with the xrootd http url.

It is recommended to alias the `aws` command to `aws --endpoint-url <url>`.

#### List buckets
- `aws s3 ls`

#### Create bucket
- `aws s3api create-bucket --bucket <name>`

#### Copy file
- `aws s3 cp <file> s3://<bucket>/<key>`
- `aws s3 cp s3://<bucket>/<key> <file>`

#### List files
- `aws s3 ls <bucket>`
