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

s3.config /xrootd/s3config
s3.region us-east-1
s3.service s3
s3.multipart /xrootd/mtpu
```

The two data and auth directories need to exist.

The s3config directory tree looks like this:

```
s3config/
├── buckets
│   ├── BUCKET_NAME 
│   └── ...
├── keystore
│   ├── KEY_ID
│   └── ...
└── users
    ├── USER_ID
    │   ├── BUCKET_NAME
    │   └── ...
    └── ...
```

The folders in the buckets/ directory all have the following xattr:

- s3.path: Absolute path of the bucket
- s3.owner: user id of the bucket owner

The files in the keystore each have a s3 key id as their name, with the s3 key secret id as their content.
Additionally, they have the `s3.user` xattr wich contain the user id of they key owner.

The folder in the data/users/ directory all have an `s3.new_bucket_path` xattr with the path that a bucket created by
this user will have.
Additionally, the `BUCKET_NAME` file has the `s3.createdAt` xattr which stores the creation date of the bucket as a unix
timestamp.

### Create a user

To create a new key pair, you will need to create the `s3config/keystore/KEY_ID` file with the secret key as the file
content and the userid as the `s3.user` xattr.
You will also need to create the `s3config/users/USER_ID` directory if it does not exists, and fill the `s3.new_bucket_path`
xattr.
All the other files/directories will be created automatically.

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
