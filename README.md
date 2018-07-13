# myHashjoin

This project aims to explore the the law of how hash-jion works in modern processors, and give a guide for designnig a CPU+FPGA system to accerlerate the hash-join algorithms. This project can support large dataset (target on TB level), various tuple size

This project can be run on Intel and Power architecture. You can also change the tuple size which is indicated as a `granularity factor` that will change the winner between the `no-partitioning hash join` and the `partitioning hash joni`.

You can find our ADMS@VLDB paper `Analyzing In-Memory Hash Join: Granularity Matters` via https://dblp.uni-trier.de/db/conf/vldb/adms2017.html or http://www.adms-conf.org/

This project is modified from the project "Parallel Joins on Multi-Core" by Cagri Balkesen et al. in the Systems Group at ETH Zürich. Please find their project through https://www.systems.ethz.ch/node/334.
