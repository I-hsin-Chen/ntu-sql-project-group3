from ensemble_compilation.spn_ensemble import read_ensemble
from ensemble_compilation.graph_representation import QueryType
from evaluation.utils import parse_query
from evaluation.cardinality_evaluation import GenCodeStats
import logging
logger = logging.getLogger(__name__)

def get_cardinality(ensemble_location, query, schema,
                    rdc_spn_selection, pairwise_rdc_path, use_generated_code=False,
                    max_variants=1, merge_indicator_exp=False, exploit_overlapping=False):
    
    if schema is None:
        from schemas.imdb.schema import gen_job_light_imdb_schema
        schema = gen_job_light_imdb_schema('../ssb-benchmark') 
    spn_ensemble = read_ensemble(ensemble_location, build_reverse_dict=True)
    
    query_str = query.strip()
    logger.debug(f"Predicting cardinality for query: {query_str}")
    
    if use_generated_code:
        spn_ensemble.use_generated_code()
    
    query = parse_query(query_str.strip(), schema)
    assert query.query_type == QueryType.CARDINALITY
    gen_code_stats = GenCodeStats()
    
    _, factors, cardinality_predict, factor_values = spn_ensemble \
            .cardinality(query, rdc_spn_selection=rdc_spn_selection, pairwise_rdc_path=pairwise_rdc_path,
                         merge_indicator_exp=merge_indicator_exp, max_variants=max_variants,
                         exploit_overlapping=exploit_overlapping, return_factor_values=True,
                         gen_code_stats=gen_code_stats)
    
    logger.debug(f"Predicted cardinality: {round(cardinality_predict, 2)}")
    return cardinality_predict