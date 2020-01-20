package edu.caltech.nanodb.queryeval;


import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.relations.JoinType;
import java.util.*;

import edu.caltech.nanodb.expressions.PredicateUtils;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.ColumnInfo;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = LogManager.getLogger(
        CostBasedJoinPlanner.class);


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @param enclosingSelects the list on enclosing select clauses
     *
     * @return a plan tree for executing the specified query
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) {

        //
        // This is a very rough sketch of how this function will work,
        // focusing mainly on join planning:
        //
        // 1)  Pull out the top-level conjuncts from the FROM and WHERE
        //     clauses on the query, since we will handle them in special ways
        //     if we have outer joins.
        //
        // 2)  Create an optimal join plan from the top-level from-clause and
        //     the top-level conjuncts.
        //
        // 3)  If there are any unused conjuncts, determine how to handle them.
        //
        // 4)  Create a project plan-node if necessary.
        //
        // 5)  Handle other clauses such as ORDER BY, LIMIT/OFFSET, etc.
        //
        // Supporting other query features, such as grouping/aggregation,
        // various kinds of subqueries, queries without a FROM clause, etc.,
        // can all be incorporated into this sketch relatively easily.

        PlanNode plan = null;

        FromClause fromClause = selClause.getFromClause();

        // implementation of our ExpressionProcessor
        AggregateFinder processor = new AggregateFinder();

        // Generate copied list of enclosingSelects with the passed selClause
        ArrayList<SelectClause> enclosingSelects2 = new ArrayList<>();
        if (enclosingSelects != null) {
            for (SelectClause sel : enclosingSelects) {
                enclosingSelects2.add(sel);
            }
        }
        enclosingSelects2.add(selClause);

        // Create the ExpressionPlanner processor.  Pass enclosing selects plus the
        // current select value (as it will be an enclosing select for any subqueries)
        ExpressionPlanner processor2 = new ExpressionPlanner(this,
                enclosingSelects2);

        List<SelectValue> selectValues = selClause.getSelectValues();

        HashSet<Expression> conjuncts = new HashSet<>();
        for (SelectValue sv : selectValues) {
            // Skip select-values that aren't expressions
            if (!sv.isExpression())
                continue;

            // Traverse using both processors
            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
            e.traverse(processor2);
        }
        // create Map for use in the processor
        Map<String, FunctionCall> colMap = processor.initMap();
        // collect the group by expressions
        List<Expression> groupByExprs = selClause.getGroupByExprs();

        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null){
            whereExpr.traverse(processor);
            whereExpr.traverse(processor2);
            // put the conjucts in our HashSet
            PredicateUtils.collectConjuncts(whereExpr, conjuncts);

            if (!processor.aggregates.isEmpty()) {
                throw new InvalidSQLException("Can't have aggregates in WHERE\n");
            }
        }
        // create list of aggregates
        List<Expression> aggregates = new ArrayList<>();


        Expression havingExpr = selClause.getHavingExpr();
        // moving conjuncts from conjucts list to list of aggregates if the conjunct
        // is an aggregate function call
        if (havingExpr != null){
            for (Expression c : conjuncts) {
                for (FunctionCall aggregateFunction : processor.functionCalls) {
                    if (c instanceof FunctionCall && c.equals(aggregateFunction)) {
                        conjuncts.remove(c);
                        aggregates.add(aggregateFunction);
                    }
                }
            }
        }

        // case for when no From clause is present. Creates a ProjectNode.
        if (fromClause == null) {
            plan = new ProjectNode(selClause.getSelectValues());
            plan.prepare();
            return plan;
        }

        // handle a from clause dealing with a join
        else {
            /* create a join plan using our from clause and conjuncts
             * enclosingSelects is passed in addition to enclosingSelects2 as
             * enclosingSelects2 is needed to create ExpressionPlanner processors,
             * and enclosingSelects is needed when the current set of enclosing
             * queries is needed.
             */
            JoinComponent joinComponent = makeJoinPlan(fromClause, conjuncts,
                    enclosingSelects2, enclosingSelects);

            // no longer need the conjuncts that were used in makeJoinPlan above
            conjuncts.removeAll(joinComponent.conjunctsUsed);
            plan = joinComponent.joinPlan;

            // Correct the column order if we have a trivial project

            Schema actualSchema = plan.getSchema();
            Schema desiredSchema = selClause.getFromSchema();

            if (selClause.isTrivialProject()) {
                if (!desiredSchema.getColumnInfos().equals(actualSchema.getColumnInfos())) {
                    List<SelectValue> projections = new ArrayList<>();
                    for (ColumnInfo colInfo : desiredSchema.getColumnInfos()) {
                        projections.add(new SelectValue(new ColumnValue(colInfo.getColumnName()))
                        );
                    }
                    plan = new ProjectNode(plan, projections);
                }
            }

        }

        // create HashedGroupAggregateNode for our plan, as directed
        if (!groupByExprs.isEmpty() || !colMap.isEmpty()){
            plan = new HashedGroupAggregateNode(plan, groupByExprs, colMap);
        }

        // Throw an error if there is a Subquery in the GROUP BY clause.
        for (Expression e : groupByExprs) {
            if (e != null && !(e instanceof ColumnValue)) {
                SubqueryOperator GBo = (SubqueryOperator) e;
                if (GBo.getSubquery() != null) {
                    throw new IllegalStateException("GROUP BY cannot contain a Subquery");
                }
            }
        }

        havingExpr = selClause.getHavingExpr();
        if (havingExpr != null) {
            // Traverse the having expression with both processors
            havingExpr.traverse(processor);
            havingExpr.traverse(processor2);
            selClause.setHavingExpr(havingExpr);
            plan = PlanUtils.addPredicateToPlan(plan, havingExpr);
        }

        if (!selClause.isTrivialProject()) {
            plan = new ProjectNode(plan, selectValues);
        }

        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (orderByExprs.size() > 0){
            // need to do something about order by clause.
            plan = new SortNode(plan, orderByExprs);
        }

        // Throw an error if there is a Subquery in the ORDER BY clause.
        for (OrderByExpression e : orderByExprs) {
            Expression exp = e.getExpression();
            if (exp != null && !(exp instanceof ColumnValue)) {
                SubqueryOperator GBo = (SubqueryOperator) exp;
                if (GBo.getSubquery() != null) {
                    throw new IllegalStateException("ORDER BY cannot contain a Subquery");
                }
            }
        }

        // Check if there was a subquery found
        if (processor2.getFoundSubQuery()) {
            plan.setEnvironment(processor2.getEnvironment());
        }

        plan.prepare();
        return plan;
    }


    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @param enclosingSelects the list of selects to be passed to create any
     *         ExpressionPlanner processors.  This list includes the enclosing
     *         selects of the current query plus the current select clause
     * @param prevEncloseSelects the list of enclosing selects of the current
     *         query
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts, List<SelectClause> enclosingSelects,
                                       List<SelectClause> prevEncloseSelects) {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<>();

        collectDetails(fromClause, conjuncts, leafFromClauses, enclosingSelects);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts, enclosingSelects, prevEncloseSelects);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:\n" +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts, enclosingSelects);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
        PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause.  If the fromClause is a base table,
     * a subquery (derived table), or an outer-join, then it is treated as
     * a leaf.  If, however, it is a join that isn't an outer-join, then
     * the conjuncts are collected from the expression, and collectDetails
     * is called on its children.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param enclosingSelects the list of selects to be passed to create any
     *         ExpressionPlanner processors.  This list includes the enclosing
     *         selects of the current query plus the current select clause
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses,
                                List<SelectClause> enclosingSelects) {
        /*
         * Check if the plan is a base table, derived table, our outer-join, and if
         * so add the from clause to leafFromClauses.
         */
        if (fromClause.isBaseTable()) {
            leafFromClauses.add(fromClause);
        }
        else if (fromClause.isDerivedTable()) {
            leafFromClauses.add(fromClause);
        }
        else if (fromClause.isOuterJoin()) {
            leafFromClauses.add(fromClause);
        }
        /* Otherwise, if the fromClause is a join expression that isn't outer
         * collect the conjuncts on its expression and collectDetails on its
         * children.
         */
        else if (fromClause.isJoinExpr()){

            // Create a processor to check for subqueries in the FROM
            ExpressionPlanner proccessor3 = new ExpressionPlanner(this,
                    enclosingSelects);

            fromClause.getOnExpression().traverse(proccessor3);

            if (proccessor3.getFoundSubQuery()) {
                // Throw an exception if a subquery is found
                throw new InvalidSQLException("Unsupported Feature Error");
            }

            PredicateUtils.collectConjuncts(fromClause.getOnExpression(), conjuncts);
            collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses,
                    enclosingSelects);
            collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses,
                    enclosingSelects);
        }
        else {
            collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses,
                    enclosingSelects);
            collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses,
                    enclosingSelects);
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @param enclosingSelects the list of selects to be passed to create any
     *         ExpressionPlanner processors.  This list includes the enclosing
     *         selects of the current query plus the current select clause
     *
     * @param prevEncloseSelects the list of enclosing selects of the current
     *         query
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts,
        List<SelectClause> enclosingSelects, List<SelectClause> prevEncloseSelects) {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = new ArrayList<>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<>();
            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts, enclosingSelects,
                        prevEncloseSelects);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     * If the fromClause if a base table, create a FileScan node.
     * If the fromClause is a derived table, makePlan on its subquery.
     * If the fromClause is an outer-join, generate plans for its children
     * and create a new NestedLoopJoinNode to combine them.
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts
     *        applied in this plan from the <tt>conjuncts</tt> collection
     *        should be added to this out-param.
     *
     * @param enclosingSelects the list of selects to be passed to create any
     *         ExpressionPlanner processors.  This list includes the enclosing
     *         selects of the current query plus the current select clause
     *
     * @param prevEncloseSelects the list of enclosing selects of the current
     *         query
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts,
                                  List<SelectClause> enclosingSelects,
                                  List<SelectClause> prevEncloseSelects) {

        // Create copy of the enclosingSelects
        ArrayList<SelectClause> enclosingSelects2 = new ArrayList<>();
        if (enclosingSelects != null) {
            for (SelectClause sel : enclosingSelects) {
                enclosingSelects2.add(sel);
            }
        }

        // Create the ExpressionPlanner processor
        ExpressionPlanner processor2 = new ExpressionPlanner(this,
                enclosingSelects2);

        /*
         * If the fromClause is a base table then create a FileScanNode
         * Make sure to set the predicate only to the conjuncts accepted
         * by the schema.
         */
        if (fromClause.isBaseTable()) {
            TableInfo tableInfo = storageManager.getTableManager().openTable(fromClause.getTableName());
            PlanNode fromnode = new FileScanNode(tableInfo, null);
            if(fromClause.isRenamed()){
                fromnode = new RenameNode(fromnode, fromClause.getResultName());
            }
            fromnode.prepare();
            HashSet<Expression> acceptedConjuncts = new HashSet<>();

            // need to account for schemas of parents
            if (prevEncloseSelects != null) {
                if (!prevEncloseSelects.isEmpty()) {
                    ArrayList<Schema> schemList = new ArrayList<>();
                    for (SelectClause sel : prevEncloseSelects) {
                        schemList.add(sel.getFromSchema());
                    }
                    schemList.add(fromnode.getSchema());

                    Schema [] schemArr = schemList.toArray(new Schema[schemList.size()]);
                    PredicateUtils.findExprsUsingSchemas(conjuncts, false, acceptedConjuncts, schemArr);
                }
                else {
                    PredicateUtils.findExprsUsingSchemas(conjuncts, false, acceptedConjuncts,
                            fromClause.getSchema());
                }
            }
            else {
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, acceptedConjuncts,
                        fromClause.getSchema());
            }

            // Add the acceptedConjuncts to the leaf and create the predicate
            leafConjuncts.addAll(acceptedConjuncts);
            Expression predicate = PredicateUtils.makePredicate(acceptedConjuncts);
            if (predicate != null) {
                // Deal with subqueries appropriately
                predicate.traverse(processor2);
                fromnode.setEnvironment(processor2.getEnvironment());
                fromnode = PlanUtils.addPredicateToPlan(fromnode, predicate);
                if (processor2.getFoundSubQuery()) {
                    fromnode.setEnvironment(processor2.getEnvironment());
                }
                fromnode.prepare();
            }
            return fromnode;
        }
        /*
         * If the fromClause is a derived table, call makePlan on the subquery
         * Make sure to set the predicate only to the conjuncts accepted by the
         * schema.
         */
        else if (fromClause.isDerivedTable()) {
            PlanNode subnode = makePlan(fromClause.getSelectClause(), null);
            if(fromClause.isRenamed()){
                subnode = new RenameNode(subnode, fromClause.getResultName());
            }
            subnode.prepare();
            HashSet<Expression> acceptedConjuncts = new HashSet<>();
            PredicateUtils.findExprsUsingSchemas(conjuncts, false, acceptedConjuncts, subnode.getSchema());
            leafConjuncts.addAll(acceptedConjuncts);
            Expression predicate = PredicateUtils.makePredicate(acceptedConjuncts);
            if (predicate != null) {
                predicate.traverse(processor2);
                subnode = PlanUtils.addPredicateToPlan(subnode, predicate);
                if (processor2.getFoundSubQuery()) {
                    subnode.setEnvironment(processor2.getEnvironment());
                }
                subnode.prepare();
            }
            return subnode;
        }
        /*
         * If the fromClause is an OuterJoin, then create JoinComponents for
         * the children, find the conjuncts used for the sides of the join
         * that aren't outer.  Then create a new NestedLoopJoinNode
         * and use the conjuncts in the on expression minus the conjuncts
         * used for the children.
         */
        else if (fromClause.isOuterJoin()) {
            JoinComponent leftplan;
            JoinComponent rightplan;
            HashSet<Expression> leftacceptedConjuncts = new HashSet<>();
            HashSet<Expression> rightacceptedConjuncts = new HashSet<>();
            HashSet<Expression> acceptedConjuncts = new HashSet<>();

            ExpressionPlanner proccessor3 = new ExpressionPlanner(this,
                    enclosingSelects);

            fromClause.getOnExpression().traverse(proccessor3);

            if (proccessor3.getFoundSubQuery()) {
                throw new InvalidSQLException("Unsupported Feature Error");
            }

            acceptedConjuncts.add(fromClause.getOnExpression());

            // Deal with different Join types
            if (!fromClause.hasOuterJoinOnRight()) {
                leftplan = makeJoinPlan(fromClause.getLeftChild(), conjuncts, enclosingSelects,
                        prevEncloseSelects);
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, leftacceptedConjuncts,
                        leftplan.joinPlan.getSchema());
            }
            else {
                leftplan = makeJoinPlan(fromClause.getLeftChild(), null, enclosingSelects,
                        prevEncloseSelects);
            }
            if (!fromClause.hasOuterJoinOnLeft()) {
                rightplan = makeJoinPlan(fromClause.getRightChild(), conjuncts, enclosingSelects,
                        prevEncloseSelects);
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, rightacceptedConjuncts,
                        rightplan.joinPlan.getSchema());
            }
            else {
                rightplan = makeJoinPlan(fromClause.getRightChild(), null, enclosingSelects,
                        prevEncloseSelects);
            }

            // Create the new NestedLoopJoinNode
            PlanNode outernode = new NestedLoopJoinNode(leftplan.joinPlan, rightplan.joinPlan,
                    fromClause.getJoinType(), null);
            outernode.prepare();


            PredicateUtils.findExprsUsingSchemas(conjuncts, false, acceptedConjuncts, outernode.getSchema());
            acceptedConjuncts.removeAll(leftacceptedConjuncts);
            acceptedConjuncts.removeAll(rightacceptedConjuncts);
            leafConjuncts.addAll(acceptedConjuncts);
            Expression predicate = PredicateUtils.makePredicate(acceptedConjuncts);
            if (predicate != null) {
                predicate.traverse(processor2);
                if (processor2.getFoundSubQuery()) {
                    outernode.setEnvironment(processor2.getEnvironment());
                }
            }
            ((NestedLoopJoinNode) outernode).predicate = predicate;
            outernode.prepare();

            return outernode;
        }

        return null;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @param enclosingSelects the list of selects to be passed to create any
     *         ExpressionPlanner processors.  This list includes the enclosing
     *         selects of the current query plus the current select clause
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts,
        List<SelectClause> enclosingSelects) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans = new HashMap<>();

        ArrayList<SelectClause> enclosingSelects2 = new ArrayList<>();
        if (enclosingSelects != null) {
            for (SelectClause sel : enclosingSelects) {
                enclosingSelects2.add(sel);
            }
        }

        // Create the ExpressionPlanner processor
        ExpressionPlanner processor2 = new ExpressionPlanner(this,
                enclosingSelects2);

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<>();

            // Loop over the n-node plan and each added leaf
            for (HashMap.Entry<HashSet<PlanNode>, JoinComponent> entry : joinPlans.entrySet()) {
                for (JoinComponent leaf : leafComponents) {
                    // Make sure that the leaf isn't already in the plan
                    if (entry.getKey().contains(leaf.joinPlan)) {
                        continue;
                    }

                    HashSet<Expression> passedConjuncts = new HashSet<>();
                    HashSet<Expression> unusedConjuncts = new HashSet<>(conjuncts);

                    // Remove the conjuncts in the leaf and plans from the unusedConjuncts
                    unusedConjuncts.removeAll((HashSet<Expression>) leaf.conjunctsUsed.clone());
                    unusedConjuncts.removeAll((HashSet<Expression>) entry.getValue().conjunctsUsed.clone());

                    // Determine the subset of unusedConjuncts that apply to the schemas.
                    PredicateUtils.findExprsUsingSchemas(unusedConjuncts, false, passedConjuncts,
                            leaf.joinPlan.getSchema(), entry.getValue().joinPlan.getSchema());

                    // Create the predicate from these conjuncts
                    Expression predicate = PredicateUtils.makePredicate(passedConjuncts);

                    // Add back the conjuncts removed
                    HashSet<Expression> totalUsedConjuncts = new HashSet<>(passedConjuncts);
                    totalUsedConjuncts.addAll(leaf.conjunctsUsed);
                    totalUsedConjuncts.addAll(entry.getValue().conjunctsUsed);

                    // Create the new NestedLoopJoinNode
                    PlanNode resultnode = new NestedLoopJoinNode(entry.getValue().joinPlan, leaf.joinPlan,
                            JoinType.INNER, predicate);

                    if (predicate != null) {
                        // Traverse the predicate to deal with any subqueries.
                        predicate.traverse(processor2);
                        resultnode.setEnvironment(processor2.getEnvironment());
                    }

                    resultnode.prepare();

                    // Create the new JoinComponent
                    HashSet<PlanNode> newKey = new HashSet<>(entry.getKey());
                    newKey.add(leaf.joinPlan);
                    JoinComponent newValue = new JoinComponent(resultnode, newKey,
                            totalUsedConjuncts);

                    // Determine if the new plan of n + 1 nodes should be added
                    PlanCost newCost = resultnode.getCost();
                    if (nextJoinPlans.containsKey(newKey)) {
                        JoinComponent value = nextJoinPlans.get(newKey);
                        if (newCost.cpuCost < value.joinPlan.getCost().cpuCost) {
                            // Add if the new plan is the best plan
                            nextJoinPlans.put(newKey, newValue);
                        }
                    }
                    else {
                        // Add if the new plan is the only plan
                        nextJoinPlans.put(newKey, newValue);
                    }
                }
            }

            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
    }
}
