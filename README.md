# Apache Spark Connect Gateway

A stateless gRPC proxy that fronts a pool of [Apache Spark Connect][spark-connect]
servers, adding session affinity, multi-tenant routing, authentication, and
observability in front of the open-source Spark Connect server.

This is an [Apache Spark][spark] subproject, developed as a Rust workspace.

> **Status: bootstrapping.** This repository is being initialized. The gateway
> implementation is imported separately; for now it contains only project
> scaffolding (license, CI, and contribution setup).

## Contributing

This subproject uses the Apache Spark development process: the
`dev@spark.apache.org` mailing list and the `SPARK` JIRA project. Please open a
pull request against `main`.

## License

Licensed under the [Apache License, Version 2.0](LICENSE); see [NOTICE](NOTICE)
for attribution details.

[spark]: https://spark.apache.org/
[spark-connect]: https://spark.apache.org/docs/latest/spark-connect-overview.html
