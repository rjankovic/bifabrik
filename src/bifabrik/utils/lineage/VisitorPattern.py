from bifabrik.utils.lineage.LineageExpressionId import LineageExpressionId
from bifabrik.utils.lineage.LineageDependency import LineageDependency
from bifabrik.utils.lineage.LineageTableColumnId import LineageTableColumnId
from typing import List
from typing import Set
from typing import Dict

"""
Add any dependencies discovered in this node to the list
Return list of sub-expressions
"""
def visit_node(node, dependencies : list[LineageDependency]) -> list[LineageExpressionId]:
    res_expressions: list[LineageExpressionId] = []
    #try:
    class_name_1 = node.getClass().getName()
    class_name_2 = node.getClass().getSuperclass().getName()
    class_name_3 = node.getClass().getSuperclass().getSuperclass().getName()
    #class_name_4 = node.getClass().getSuperclass().getSuperclass().getSuperclass().getName()
    #class_name_5 = node.getClass().getSuperclass().getSuperclass().getSuperclass().getSuperclass().getName()

    
    ####################
    # print('VISITING NODE')
    # print(class_name_1)
    # print(class_name_2)
    # print(class_name_3)
    ####################


    #print(class_name_4)
    #print(class_name_5)
    
    #####################
    # print(node)
    #####################
    


    # optimistic expectations
    handled = True
    #print(class_name_1)
    # Match the day to predefined patterns
    match class_name_1:
        case "org.apache.spark.sql.catalyst.plans.logical.Project":
            return visit_project_node(node, dependencies)
        case "org.apache.spark.sql.catalyst.plans.logical.Join":
            return visit_join_node(node, dependencies)
        case "org.apache.spark.sql.execution.datasources.LogicalRelation":
            return visit_logical_relation(node, dependencies)
        #case "org.apache.spark.sql.catalyst.expressions.Alias":
        #    return visit_alias_expression(node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.UnaryExpression":
            return visit_unary_expression(node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.Attribute":
            return visit_attribute(node, dependencies)
        case "org.apache.spark.sql.catalyst.plans.logical.WithCTE":
            return visit_cte(node, dependencies)
        case "org.apache.spark.sql.catalyst.plans.logical.CTERelationRef":
            return visit_cte_relation_ref(node, dependencies)
        case "org.apache.spark.sql.catalyst.plans.logical.Union":
            return visit_union(node, dependencies)
        case "org.apache.spark.sql.execution.LogicalRDD":
            return visit_logical_rdd(node, dependencies)
        case _:
            handled = False
            
    if handled:
        return []
    
    handled = True
    # these also appear in class_name_1 handler - not sure, but there may be some nodes that have these as the class name without any sublass specified
    match class_name_2:
        case "org.apache.spark.sql.catalyst.expressions.UnaryExpression":
            return visit_unary_expression(node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.Attribute":
            return visit_attribute(node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.BinaryOperator":
            return visit_binary_operator(node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.Expression":
            return visit_expression(node, dependencies)
        case "org.apache.spark.sql.catalyst.plans.logical.AggregateLike":
            # GROUP BY
            return visit_aggregate(node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction":
            return visit_aggregate_function(node, dependencies)
        # subqueries
        case "org.apache.spark.sql.catalyst.plans.QueryPlan":
            #print(class_name_1)
            #print(class_name_2)
            child_node = node.child()
        
            return visit_node(child_node, dependencies)
        case _:
            handled = False
    
    if handled:
        return {}

    handled = True
    match class_name_3:
        # subqueries
        case "org.apache.spark.sql.catalyst.plans.QueryPlan":
            #print(class_name_1)
            #print(class_name_2)
            child_node = node.child()
            return visit_node(child_node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.BinaryOperator":
            return visit_binary_operator(node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.BinaryExpression":
            return visit_binary_operator(node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.Expression":
            return visit_expression(node, dependencies)
        case "org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction":
            return visit_aggregate_function(node, dependencies)
        case _:
            handled = False

    return res_expressions
    # ... return the IDs that were discovered while visiting this
    # except Exception as e:
    #     # TODO
    #     print('EXCEPTION')
    #     print(e)
    #     return res_expressions
    # ... return the IDs that were discovered while visiting this


"""Get the project items as a python list from the scala project node
"""
def get_project_list_python(project_list):
    class_name_1 = project_list.getClass().getName()
    class_name_2 = project_list.getClass().getSuperclass().getName()
    class_name_3 = project_list.getClass().getSuperclass().getSuperclass().getName()

    # the project list can be either a list or a vector - get projection items in each case
    if class_name_1 == 'scala.collection.immutable.Vector':
        #print('PROJECT VECTOR')
        project_list_python_outer = [project_list.apply(i) for i in range(project_list.size())]
        
    else:
        project_list_iterator = project_list.productIterator().toIndexedSeq()
        project_list_python_outer = [project_list_iterator.apply(i) for i in range(project_list_iterator.size())]
    

    #project_list_iterator = project_list.productIterator().toIndexedSeq()
    #project_list_python_outer = [project_list_iterator.apply(i) for i in range(project_list_iterator.size())]

    project_list_python = []
    for proj_outer in project_list_python_outer:
        proj_outer_class_name = proj_outer.getClass().getName()
        if proj_outer_class_name == "scala.collection.immutable.Nil$":
            continue
        if proj_outer_class_name == "scala.collection.immutable.$colon$colon":
            proj_elems = proj_outer.toIndexedSeq()
            proj_elems_python = [proj_elems.apply(i) for i in range(proj_elems.size())]
            for proj_elem in proj_elems_python:
                project_list_python.append(proj_elem)
        else:
            project_list_python.append(proj_outer)
    
    return project_list_python

def visit_project_node(node, dependencies : list[LineageDependency]) -> list[LineageExpressionId]:
    
    #print('VISITING PROJECTION')
    
    project_list = node.projectList()
    res_expressions: list[LineageExpressionId] = []

    #print('PROJECT LIST')

    project_list_python = get_project_list_python(project_list)

    # print('PROJECT ITEMS')
    # for proj in project_list_python:
    #     print(proj)

    for proj in project_list_python:    
        proj_class_name = proj.getClass().getName()
        proj_class_name2 = proj.getClass().getSuperclass().getName()
        
        expr_id = LineageExpressionId(proj)
        

        #print(f'searching for dependencies of project item {expr_id}')
        sub_references = visit_node(proj, dependencies)
        for sub_ref in sub_references:
            dependencies.append(LineageDependency(source = sub_ref, target = expr_id))
            
            
            # dependent_node, depends_on_node, node_impact, node_dependencies
            #add_dependency(expr_id.id, sub_ref.id, )

        #res_references.add(expr_id.id)
        #res_references.update(sub_references)
        res_expressions.append(expr_id)
        res_expressions.extend(sub_references)
    
    proj_child = node.child()
    
    child_expressions = visit_node(proj_child, dependencies)
    res_expressions.extend(child_expressions)
    #res_references.update(child_refs)

    return res_expressions

def visit_join_node(node, dependencies : list[LineageDependency]) -> list[LineageExpressionId]:
    #print('VISITING JOIN')

    res_expressions: list[LineageExpressionId] = []
    
    left = node.left()
    right = node.right()

    left_sub_ref = visit_node(left, dependencies)
    right_sub_ref = visit_node(right, dependencies)

    res_expressions.extend(left_sub_ref)
    res_expressions.extend(right_sub_ref)

    return res_expressions


def visit_logical_relation(node, dependencies : list[LineageDependency]) -> list[LineageExpressionId]:
    #print('VISITING LOGICAL RELATION')
    
    res_expressions: list[LineageExpressionId] = []

    #relations.append(node)

    # print(r)

    # print(r.getClass().getName())
    # print(r.getClass().getSuperclass().getName())
    # print(r.getClass().getSuperclass().getSuperclass().getName())

    r = node
    
    table_identifier = r.catalogTable().get().identifier().toString()
    #print(table_identifier)

    

    r_pa = r.producedAttributes()
    r_pai = r_pa.toIndexedSeq()
    
    r_pal = [r_pai.apply(i) for i in range(r_pai.size())]

    for pa in r_pal:
        #print(pa)
        expr_id = LineageTableColumnId(pa, table_identifier)
        res_expressions.append(expr_id)
        #print(expr_id)
        #print(pa.getClass().getName())


    return res_expressions

def visit_logical_rdd(node, dependencies : list[LineageDependency]) -> list[LineageExpressionId]:
    # print('VISITING LOGICAL RDD')
    
    res_expressions: list[LineageExpressionId] = []

    r = node
    r_pa = r.producedAttributes()
    r_pai = r_pa.toIndexedSeq()
    
    r_pal = [r_pai.apply(i) for i in range(r_pai.size())]

    for pa in r_pal:
        expr_id = LineageExpressionId(pa)
        res_expressions.append(expr_id)
        # print(expr_id)
        # print(pa.getClass().getName())


    return res_expressions

def visit_cte_relation_ref(node, dependencies : list[LineageDependency]) -> list[LineageExpressionId]:
    #print('VISITING CTE REF')
    
    res_expressions: list[LineageExpressionId] = []

    r = node

    r_pa = r.producedAttributes()
    r_pai = r_pa.toIndexedSeq()
    
    r_pal = [r_pai.apply(i) for i in range(r_pai.size())]

    # kind of redundant, these expressions appear in the CTE definition already
    for pa in r_pal:
        #print(pa)
        expr_id = LineageExpressionId(pa)
        res_expressions.append(expr_id)

    return res_expressions


def visit_unary_expression(node, dependencies : list[LineageDependency]) -> List[LineageExpressionId]:
    #print('VISITING UNARY EXPRESSION')
    
    #help(node)
    class_name_1 = node.getClass().getName()
    res_expressions: list[LineageExpressionId] = []
    
    ch = node.child()

    child_expressions = visit_node(ch, dependencies)
    res_expressions.extend(child_expressions)
    
    if class_name_1 == "org.apache.spark.sql.catalyst.expressions.Alias":
        #print('ALIAS')
        expression_id = LineageExpressionId(node) 
        res_expressions.append(expression_id)
        for ch_exp in child_expressions:
            dependencies.append(LineageDependency(source = ch_exp, target = expression_id))
        
        #aliases.append(node)

    return res_expressions


def visit_expression(node, dependencies : list[LineageDependency]) -> List[LineageExpressionId]:
    #print('VISITING GENERAL EXPRESSION')
    
    #help(node)
    class_name_1 = node.getClass().getName()
    res_expressions: list[LineageExpressionId] = []

    children = node.children().toIndexedSeq()
    children_python = [children.apply(i) for i in range(children.size())]
    
    for ch in children_python:
        child_expressions = visit_node(ch, dependencies)
        res_expressions.extend(child_expressions)

    return res_expressions


def visit_aggregate_function(node, dependencies : list[LineageDependency]) -> List[LineageExpressionId]:
    #print('VISITING AGGREGATE FUNCTION')
    
    #help(node)
    class_name_1 = node.getClass().getName()
    res_expressions: list[LineageExpressionId] = []
    
    #ch = node.child()

    children = node.children().toIndexedSeq()
    children_python = [children.apply(i) for i in range(children.size())]

    #print(f'children count: {len(children_python)}')
    for ch in children_python:
        child_expressions = visit_node(ch, dependencies)
        res_expressions.extend(child_expressions)
    
        #aliases.append(node)

    return res_expressions


def visit_attribute(node, dependencies : list[LineageDependency]) -> List[LineageExpressionId]:
    #print('VISITING ATTRIBUTE')
    
    #class_name_1 = node.getClass().getName()
    lineage_expression_id = LineageExpressionId(node)
    res_expressions: list[LineageExpressionId] = [lineage_expression_id]
    
    return res_expressions


def visit_binary_operator(node, dependencies : list[LineageDependency]) -> List[LineageExpressionId]:
    # print('VISITING BINARY OPERATOR')
    
    #class_name_1 = node.getClass().getName()
    res_expressions: list[LineageExpressionId] = []
    
    l = node.left()
    r = node.right()

    # print('L')
    # print(l)
    # print('R')
    # print(r)

    child_expressions_left = visit_node(l, dependencies)
    child_expressions_right = visit_node(r, dependencies)
    
    res_expressions.extend(child_expressions_left)
    res_expressions.extend(child_expressions_right)
    
    #binary_operators.append(node)

    return res_expressions


def visit_aggregate(node, dependencies : list[LineageDependency]) -> List[LineageExpressionId]:
    #print('VISITING AGGREGATE')

    res_expressions: list[LineageExpressionId] = []

    a_seq = node.aggregateExpressions()
    aggregate_list_iterator = a_seq.toIndexedSeq()
    aggregate_list_python = [aggregate_list_iterator.apply(i) for i in range(aggregate_list_iterator.size())]

    for agg_item in aggregate_list_python:
        agg_class_name = agg_item.getClass().getName()
        agg_class_name2 = agg_item.getClass().getSuperclass().getName()

        expr_id = LineageExpressionId(agg_item)

        # print(f'searching for dependencies of aggregate item {expr_id}')
        # print('searching in')
        # print(agg_item)
        sub_references = visit_node(agg_item, dependencies)
        #print('sub_refs')
        for sub_ref in sub_references:
            #print(sub_ref)
            dependencies.append(LineageDependency(source = sub_ref, target = expr_id))

        res_expressions.append(expr_id)
        res_expressions.extend(sub_references)

    agg_child = node.child()

    child_expressions = visit_node(agg_child, dependencies)
    res_expressions.extend(child_expressions)


    #aggregates.append(node)

    return res_expressions

def visit_union(node, dependencies : list[LineageDependency]) -> list[LineageExpressionId]:
    #TODO: this is incomplete! only the first query in the UNION is mapped correctly
    
    # print('VISITING UNION')
    # print(node)
    class_name_1 = node.getClass().getName()
    #print(class_name_1)

    res_expressions: list[LineageExpressionId] = []

    children_python = [node]

    unwrapped = False
    while not unwrapped:
        unwrapped = True
        for i in range(0, len(children_python)):
            ch = children_python[i]
            child_type = ch.getClass().getName()
            if child_type == "org.apache.spark.sql.catalyst.plans.logical.Union":
                #print('UNION IN UNION')
                unwrapped = False
                ch_children = ch.children().toIndexedSeq()
                ch_children_python = [ch_children.apply(i) for i in range(ch_children.size())]
                children_python[i:i+1] = ch_children_python
                #print('RECHECK')
                break

    # children = node.children().toIndexedSeq()
    # children_python = [children.apply(i) for i in range(children.size())]
    
# case "org.apache.spark.sql.catalyst.plans.logical.Union":
#             return visit_union(node, dependencies)

    union_output_columns = []


    # print(f'union children: {len(children_python)}')
    
    # for ch in children_python:
    #     print('UNION CHILD')
    #     print(ch)
    #     print(ch.getClass().getName())
    #     print(ch.getClass().getSuperclass().getName())

    first_child = True
    for ch in children_python:
        ch_class = ch.getClass().getName()
        ch_super_class = ch.getClass().getSuperclass().getName()
        
        # print('UNION CHILD')
        # print(ch)
        # print(ch_class)
        # print(ch_super_class)

        # UNION children can be projections or aggregates
        if ch_super_class == "org.apache.spark.sql.catalyst.plans.logical.AggregateLike":
            # print('AGGREGATE IN UNION')
            a_seq = ch.aggregateExpressions()
            aggregate_list_iterator = a_seq.toIndexedSeq()
            aggregate_list_python = [aggregate_list_iterator.apply(i) for i in range(aggregate_list_iterator.size())]
            project_list = aggregate_list_python
        elif ch_class == "org.apache.spark.sql.catalyst.plans.logical.Distinct":
            #help(ch)
            ch_ch = ch.child()
            ch_ch_class = ch_ch.getClass().getName()
            # print(ch_ch_class)
            project_list = get_project_list_python(ch_ch.projectList())
            
        # org.apache.spark.sql.catalyst.plans.logical.Distinct
        # org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
        else: 
            project_list = get_project_list_python(ch.projectList())

        i = 0
        for proj in project_list:
            expr_id = LineageExpressionId(proj)
            
            sub_references = visit_node(proj, dependencies)
            for sub_ref in sub_references:
                #print('DEPENDENCY FROM UNION FIRST CHILD')
                dependencies.append(LineageDependency(source = sub_ref, target = expr_id))

            res_expressions.append(expr_id)
            res_expressions.extend(sub_references)

            
            # the first query in the union sets the output columns
            # all other queries are set as "inputs" of these output columns

            if first_child:
                union_output_columns.append(expr_id)
            else:
                dependencies.append(LineageDependency(source = expr_id, target = union_output_columns[i]))
            
            i = i + 1
            
        child_expressions = visit_node(ch, dependencies)
        res_expressions.extend(child_expressions)
        first_child = False


    return res_expressions

def visit_cte(node, dependencies : list[LineageDependency]) -> List[LineageExpressionId]:
    #print('VISITING CTE')
    
    #help(node)
    #class_name_1 = node.getClass().getName()
    res_expressions: list[LineageExpressionId] = []

    children = node.children().toIndexedSeq()
    children_python = [children.apply(i) for i in range(children.size())]
    
    for ch in children_python:
        child_expressions = visit_node(ch, dependencies)
        res_expressions.extend(child_expressions)

    return res_expressions