# Graph Path Feature Learning (GPFL)

GPFL is a bottom-up probabilistic inductive rule learner utilizing a Generalization-Specialization (Gen-Spec) rule generation mechanism to efficiently learn both abstract and instantiated rules directly from knowledge graphs.

## Requirements
- Java >= 1.8
- Gradle >= 5.6.4

## Getting Started
![a](https://www.dropbox.com/s/d47mh2bn7t4n7rw/GPFL-Overview.png?raw=1)

This figure shows the functionalities implemented in the GPFL system. To get started on learning probabilistic first-order rules using GPFL and reproducing results reported in the paper, let's first walk through an example where we learn rules for ALL relation types in the UWCSE dataset.

#### Build Neo4j Graph Database
As the system is implemented on top of the Neo4j Core API, it requires data being presented in a Neo4j Graph Database. We have implemented a procedure that efficiently converts a triple file into a graph database. Please download the benchmark datasets from [here](https://www.dropbox.com/s/hbgih3juiuoj34e/GPFL-Benchmarks.zip?dl=1) and unzip it into folder `experiments` at your GPFL home directory. 

Option `-t tripleFile` specifies the path of the triple file used to build a graph database. To build a graph database for UWCSE, execute:
```
gradle run --args="-t experiments/UWCSE/triples.txt"
```
This directive will generate a Neo4j graph database at `experiments/UWCSE/databases`, which can be queried by using [Neo4j Cypher](https://neo4j.com/download/) for EDA and verification.

#### Create Train/Test Sets
Option `-c configFile` specifies the path of the GPFL configuration file. You can find the config file for UWCSE at `experiments/UWCSE/config.json`. Here we introduce some of the useful keys:
- `target_relation`: a collection of relation types you want to learn rules for. For instance, if for a dataset it includes relation types `A,B,C,D,E` and you only want to learn rules for `B,E`, then simply set `target_relatoin` to `["B", "E"]`, which instructs the system to only learn rules for `B` and `E`. When setting to empty, it either learn rules for all of the relation types, or a randomly selected subsets.
- `randomly_selected_relations`: specifies the number of randomly selected relations you want to learn rules for. For instance, when setting it to 20, the system will randomly select 20 relation types from the data and learn rules for each of them. When `target_relation` has higher priority then `randomly_selected_relations`. When `target_relation` is empty and `randomly_selected_relations` is 0, the system will learn rules for all relation types in the data.
- `split_ratio`: specifies the train to test set ratio.

Option `-s` creates train/test sets for relation types to be learnt. To create train/test sets with a split ratio of 0.7 for all relation types in UWCSE, execute: 
```
gradle run --args="-c experiments/UWCSE/config.json -s"
```
The generated train/test files can be found at `experiments/UWCSE/relation_type_name/`.

#### Learn Rules with Consistent Train/Test Set
Option `-r` triggers the learning of rules. To learn rules for all relation types in UWCSE with previously generated train/test sets, execute:
```
gradle run --args="-c experiments/UWCSE/config.json -r"
```
This will generate several files for each relation type:
- `predictions.txt`: the top-10 predictions for queries.
- `rules.txt`: learnt rules sorted by confidence in descending order
- `verifications.txt`: top 20 predictions of queries and top 10 rules suggesting each of the predictions
The number of top-n prediction and rules can be fine-tuned by modifying static fields in `src\main\java\ac\uk\ncl\Settings.java`.

#### Learn Rules with Re-split Train/Test Set
For experiments purpose, we want to re-split train/test set for every run. To learn rules for all relation types in UWCSE with re-split train/test sets, execute:
```
gradle run --args="-c experiments/UWCSE/config.json -r -f"
```

## Reproduce Experiment Results
This version of GPFL is an in-memeory implementation. To run GPFL on following benchmarks, the running machine should have at least 6 CPU cores and 64GB RAM. All of our experiments are conducted on AWS EC2 r5.2xlarge instances. The experiment results reported in the paper are mean and std of results over 10 runs.

#### FB15K-237
```
gradle run --args="-c experiments/FB15K-237/config.json -r -f"
```
Runtime: ~11375s

#### WN18RR
```
gradle run --args="-c experiments/WN18RR/config.json -r -f"
```
Runtime: ~2641s

#### NELL-995
```
gradle run --args="-c experiments/NELL995/config.json -r -f"
```
Runtime: ~4101s

## Citation
If you use our code, please cite the paper:
```
@article{gu2020efficient,
  title={Efficient Rule Learning with Template Saturation for Knowledge Graph Completion},
  author={Gu, Yulong and Guan, Yu and Missier, Paolo},
  journal={arXiv preprint arXiv:2003.06071},
  year={2020}
}
```

## License
GPFL is available free of charge for academic research and teaching only. If you intend to use GPFL for commercial purposes then you MUST contact the authors vis email y.gu11@newcaslte.ac.uk 

