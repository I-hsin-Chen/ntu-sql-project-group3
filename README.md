# NTU EE5178 Group3 -- Query Optimization through Cardinality Estimation
2024 NTU Project `資料庫系統-從SQL到NoSQL Database Management System — from SQL to NoSQL` Group 3


* Check original [DeepDB](https://github.com/DataManagementLab/deepdb-public.git) repository to set up your environment.
* This [Google Drive](https://drive.google.com/drive/folders/11e19PH7jNOBdFVNzegg0uJcrT2xFCpTy) link contains pretrained imdb and pkl weights of it.
* Our main effort is in the folder `parser`. Below is the file descriptions of `parser`.
<img src="https://i.imgur.com/aAhTXIR.jpeg" width="80%">

### Cardinality estimation command

If you want to estimate the cardinality of a specific query. Run the following command :
```
python maqp.py --get_cardinality ^
    --rdc_spn_selection ^
    --query "SELECT COUNT(*) FROM movie_companies mc,title t,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mi_idx.info_type_id=112 AND mc.company_type_id=2;" ^
    --max_variants 1 ^
    --pairwise_rdc_path imdb-benchmark/spn_ensembles/pairwise_rdc.pkl ^
    --dataset imdb-light ^
    --ensemble_location imdb-benchmark/spn_ensembles/ensemble_relationships_imdb-light_1000000.pkl ^
    --query_file_location benchmarks/job-light/sql/job_light_queries.sql
```
Change `--ensemble_location` and `--pairwise_rdc_path` to your `.pkl` file location.

### Three algorithms evaluation
If you want to evaluate performance of algorithms of specific queries, first go to the folder `parser` : 
```
cd parser
```
Add the desire query string in `queries = []` in main function, then run : 
```
python run_with_sqlite.py
```
* Note that you have add your two `.pkl` files in `./imdb-benchmark/spn_ensembles` and `imdb.db` in the parent folder.
* The result will be generated in `output.csv`.
