# DeepDB: DeepDB: Learn from Data, not from Queries!
Original [DeepDB](https://github.com/DataManagementLab/deepdb-public.git) repository
Our main effort is in the folder `parser`
This [Google Drive](https://drive.google.com/drive/folders/11e19PH7jNOBdFVNzegg0uJcrT2xFCpTy) link contains imdb and pkl weights of it.

### Command

Change `--ensemble_location` and `--pairwise_rdc_path` to your `.pkl` file location.

Generate evaluation results :
```
python maqp.py --evaluate_cardinalities ^
    --rdc_spn_selection ^
    --max_variants 1 ^
    --pairwise_rdc_path ../imdb-benchmark/spn_ensembles/pairwise_rdc.pkl ^
    --dataset imdb-light ^
    --target_path ./baselines/cardinality_estimation/results/deepDB/imdb_light_model_based_budget_5.csv ^
    --ensemble_location ../imdb-benchmark/spn_ensembles/ensemble_relationships_imdb-light_1000000.pkl ^
    --query_file_location ./benchmarks/job-light/sql/job_light_queries.sql ^
    --ground_truth_file_location ./benchmarks/job-light/sql/job_light_true_cardinalities.csv
```

Get the cardinality estimation of a single query :
```
python maqp.py --get_cardinality ^
    --rdc_spn_selection ^
    --query "SELECT COUNT(*) FROM movie_companies mc,title t,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mi_idx.info_type_id=112 AND mc.company_type_id=2;" ^
    --max_variants 1 ^
    --pairwise_rdc_path ../imdb-benchmark/spn_ensembles/pairwise_rdc.pkl ^
    --dataset imdb-light ^
    --ensemble_location ../imdb-benchmark/spn_ensembles/ensemble_relationships_imdb-light_1000000.pkl ^
    --query_file_location ./benchmarks/job-light/sql/job_light_queries.sql
```
