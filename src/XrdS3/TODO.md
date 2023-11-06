# Must do
- Global store/auth?
- Finish refactor
- Verify date when authorizing 
- Possible to create objects with name ending with a '/'
- Possible to create object and dir with same name?
- request id header?
- metadata
- key name: utf8, 1024 bytes max
- limit header size (8KB)?
- combine headers if same name?
- combine query params if same name?
- verify signature once body has been read (if no body check against empty hash signature)
- user metadata max 2KB
- parse all metadata headers (check all headers for put request)
- checksum headers
- handle versions and version headers
- copy object: x-amz-metadata-directive
- get object: ranges/parts

- copy operation
- acl
- versioning
- anonymous
- multipart upload

- Maybe only support Directory Buckets at the beginning, would simplify stuff
- Common function for all list operations, including list multipart uploads

- check `test_object_raw_put_authenticated_expired` test

- lock fs operations
- If we move to C++17: stringview

# Get more info
- SIGV2
- SIGV4A
- SOAP
