# go-sticky-worker-pool
![Coverage](https://img.shields.io/badge/Coverage-94.2%25-brightgreen)

This is a very simple implementation of key-sticky concurrency over go routines.

Imagine you have an FIFO queue and you want to run jobs concurrently keeping ordering. Well, you can't guarantee though, but you can guarantee ordering of correlated workloads (e.g. workloads of the same user id, or the same aggregate id).
Besides that, it may reduce your lock management overhead, since correlated jobs will run in order, and possibly will help you avoid deadlocks.

This is achieved by using a consistent hash that delivers workloads with the same key to the same channel.
