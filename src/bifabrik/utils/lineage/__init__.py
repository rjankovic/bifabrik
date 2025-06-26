"""Data lineage module
Track and save data lineage information for dataframes saved to the lakehouse as tables.
"""
import notebookutils.mssparkutils

import time
from datetime import datetime
from typing import List
import json
from typing import List
from typing import Set
from typing import Dict
import traceback

import bifabrik.utils.fsUtils as fsu

from bifabrik.utils.lineage.LineageContext import LineageContext
from bifabrik.utils.lineage.LineageExpressionId import LineageExpressionId
from bifabrik.utils.lineage.DataFrameLineage import DataFrameLineage
from bifabrik.utils.lineage.DataFrameLineageOutputColumn import DataFrameLineageOutputColumn
from bifabrik.utils.lineage.LineageDependency import LineageDependency
from bifabrik.utils.lineage.LineageSerializationHelper import LineageSerializationHelper
from bifabrik.utils.lineage.VisitorPattern import visit_node

"""Save dataframe lineage information to the lakehouse as JSON."""
def save_df_lineage(df, targetTableName: str = None, targetLakehouseName: str = None, outputFolder = None):
    current_lakehouse = fsu.currentLakehouse()
    if current_lakehouse is None:
        print('Warning: No default lakehouse is not set, skipping data lineage')
        return
    
    #print('getting lineage information for dataframe')
    df_lineage = get_df_lineage(df, targetTableName, targetLakehouseName)
    if df_lineage is None:
        # this should never happen, but just in case
        return
    #print('OK')

    out_lakehouse_name = df_lineage.context.targetLakehouseName
    out_table_name = df_lineage.context.targetTableName
    if out_table_name is None:
        out_table_name = '__UNKNOWN_TABLE__'
    
    ts = df_lineage.context.executionDateTime
    ts_formatted = formatted = ts.strftime("%Y%m%d_%H%M%S") + f"_{ts.microsecond // 1000:03d}"
    file_name = f"{out_lakehouse_name}_{out_table_name}_{ts_formatted}.json"

    #print(f'Saving lineage information for {out_lakehouse_name}.{out_table_name} to {file_name}')
    logging_lh_path = current_lakehouse['basePath']

    targetFolderPath = fsu.normalizeAbfsFilePath(outputFolder, lakehouseBasePath = logging_lh_path)
    path = f'{outputFolder}/{file_name}'
    targetFilePath = fsu.normalizeAbfsFilePath(path, lakehouseBasePath = logging_lh_path)

    #print(f'Lineage file path: {targetFilePath}')
    notebookutils.mssparkutils.fs.mkdirs(targetFolderPath)
    lineage_json = df_lineage.to_json()
    notebookutils.mssparkutils.fs.put(targetFilePath, lineage_json, True)
    #print(f'Lineage information saved to {targetFilePath}')

"""Get dataframe lineage information as an object from the analyzed plan of the dataframe."""
def get_df_lineage(df, targetTableName: str = None, targetLakehouseName: str = None) -> DataFrameLineage:
    query_execution = df._jdf.queryExecution()
    analyzed_plan = query_execution.analyzed()

    analyzed_plan_str = str(analyzed_plan)
    target_lakehouse_name = targetLakehouseName
    target_table_name = targetTableName

    try:
        df_outputs = get_df_outputs(analyzed_plan)

        node_dependencies : list[LineageDependency] = []

        # for output in df_outputs:
        #     print(output)

        # apply the visitor pattern to the analyzed plan
        sub_references = visit_node(analyzed_plan, node_dependencies)

        # different nodes can have the same expression ID - LineageColumnId(s) (logical relation columns) take priority
        # afther that, longer SQL takes priority
        sub_references_ranked = sorted(sub_references, key=lambda x: (not(type(x).__name__ == "LineageTableColumnId"), -1 * len(x.sql)))

        # one expression ID can appear multiple times in the analyzed plan
        # only keep one instance - preferrably a table reference
        expressions_cleared: List[LineageExpressionId] = []
        expression_ids = set()

        # print('-----------')
        # print('EXPRESSIONS')
        # print('-----------')

        for expr in sub_references_ranked:
            #print(type(expr).__name__)
            #print(expr)
            if expr.id not in expression_ids:
                expressions_cleared.append(expr)
                expression_ids.add(expr.id)

        # for e in expressions_cleared:
        #     print(e)

        # print('------------')
        # print('DEPENDENCIES')
        # print('------------')

        dependencies_cleared = []
        dependencis_covered = set()

        # dependencies were discovered in the visitor pattern - aliases, expression sub-nodes etc.
        for dependency in node_dependencies:
            source_id = dependency.source.id
            target_id = dependency.target.id
            dep_tuple = (source_id, target_id)
            if dep_tuple in dependencis_covered:
                continue
            if source_id == target_id:
                continue
            dependencies_cleared.append(dependency)
            dependencis_covered.add(dep_tuple)


        # for dependency in dependencies_cleared:
        #     print(dependency)

        # find the transitive closure of dependencies for the output nodes
        # that is, all the dependencies leading to each output node

        # for each output, the list of its dependencies
        dependency_lists: Dict[int, List[LineageExpressionId]] = {}
        # dependencies grouped by destination (target) ID
        dependencies_by_destination: Dict[int, List[LineageDependency]] = {}

        for dependency in dependencies_cleared:
            if dependency.target.id not in dependencies_by_destination:
                dependencies_by_destination[dependency.target.id] = [dependency]
            else:
                dependencies_by_destination[dependency.target.id].append(dependency)

        expressions_cleared_by_id = {}
        for exp in expressions_cleared:
            expressions_cleared_by_id[exp.id] = exp

        # print('-------------------')
        # print('OUTPUT DEPENDENCIES')
        # print('-------------------')
    

        output_columns: list[DataFrameLineageOutputColumn] = []

        for output in df_outputs:
            #print(output)
            dependency_lists[output.id] = []
            covered_nodes = set()
            current_front_nodes = [output]
            while len(current_front_nodes) > 0:
                search_from_node = current_front_nodes.pop()
                covered_nodes.add(search_from_node.id)
                if search_from_node.id in dependencies_by_destination:
                    for dep in dependencies_by_destination[search_from_node.id]: 
                        if dep.source.id not in covered_nodes:
                            primary_expression = expressions_cleared_by_id[dep.source.id]
                            dependency_lists[output.id].append(primary_expression)
                            current_front_nodes.append(dep.source)
                            #print(f'--- {primary_expression}')
            
            # in some cases, there is no dependency - the column goes straight from the table (SELECT C FROM TABLE)
            # for those direct outputs, add a dependency to get the primary logical relation reference (using the expression ranking)
            
            # however, do not preface this with the following condition
            ### if len(dependency_lists[output.id]) == 0:
            # wince this "direct dependency" can occur in a UNION as well, where cle column already has a source, but not the one from the first query of the UNION
            
            if output.id in expressions_cleared_by_id:
                primary_expression = expressions_cleared_by_id[output.id]
                dependency_lists[output.id].append(primary_expression)
                    #print(f'--- {primary_expression}')

            output_columns.append(DataFrameLineageOutputColumn(output, dependency_lists[output.id]))
        
        result = DataFrameLineage(columns = output_columns, dependencies = dependencies_cleared, expressions = expressions_cleared
        ,logicalPlan = analyzed_plan_str, targetLakehouseName = target_lakehouse_name, targetTableName = target_table_name)
    
        return result

    except Exception as ex:
        stack_trace = ''.join(traceback.format_tb(ex.__traceback__))
        msg = f'{str(ex)}'
        stack_trace = stack_trace.replace("'", "\\\'")
        msg = msg.replace("'", "\\\'")
        full_msg = f'{msg}\n\nSTACK TRACE\n{stack_trace}'

        result = DataFrameLineage(logicalPlan = analyzed_plan_str, targetLakehouseName = target_lakehouse_name, targetTableName = target_table_name, error = full_msg)
        
        return result

def get_df_outputs(analyzed_plan) -> List[LineageExpressionId]:
    output_seq = analyzed_plan.output()
    output_list = [output_seq.apply(i) for i in range(output_seq.size())]
    res = list()
    for o in output_list:
        output_obj = LineageExpressionId(o)
        res.append(output_obj)
    return res