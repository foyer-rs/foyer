# RisingWave

[RisingWave](https://github.com/risingwavelabs/risingwave) is a real-time event streaming platform based on S3. This blog will introduce how RisingWave uses foyer to improve performance and reduce costs.

## Background

RisingWave's storage engine, Hummock, is a distributed LSM-Tree storage engine based on S3. Just like most data infrastructure that uses S3 for storage, RisingWave has the following advantages and disadvantages when using S3 as storage:

- **Pros:**
    1. *Simplified design:* Using S3 as shared storage and the source of truth can greatly simplify the design of distributed storage systems, eliminating the need to spend significant effort on obscure consistency protocols and complex fault tolerance.
    2. *High availability:* Fully leverage S3's high availability and durability to ensure service SLA.
    3. *Strong scalability:* S3's scalability is virtually unlimited. Storage can be easily scaled out at any time as the service grows.
    4. *Lower storage costs:* Compared with other storage services, S3 has lower storage costs. The larger the amount of data, the greater the cost advantage.
- **Cons:**
    1. *High latency:* S3’s latency is several orders of magnitude higher compared to other storage services (such as local NVME drives, EBS, etc.). Without optimization, this will lead to overall system performance degradation.
    2. *High access cost:* S3 charges based on the number of accesses. Without optimization, frequent S3 access will negate the storage cost advantage, or even make it worse.

## 



***TBC ... ...***
