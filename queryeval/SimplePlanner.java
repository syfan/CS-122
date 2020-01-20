package edu.caltech.nanodb.queryeval;


import java.util.Collection;
import java.util.List;
import java.util.*;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;

import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;
import org.stringtemplate.v4.misc.Aggregate;


/**
 * This class generates execution plans for very simple SQL
 * <tt>SELECT * FROM tbl [WHERE P]</tt> queries.  The primary responsibility
 * is to generate plans for SQL <tt>SELECT</tt> statements, but
 * <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also use this class
 * to generate simple plans to identify the tuples to update or delete.
 */
public class SimplePlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = LogManager.getLogger(SimplePlanner.class);


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
                             List<SelectClause> enclosingSelects) {

        // For HW1, we have a very simple implementation that defers to
        // makeSimpleSelect() to handle simple SELECT queries with one table,
        // and an optional WHERE clause.

        PlanNode plan = null;

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not implemented:  enclosing queries");
        }

        FromClause fromClause = selClause.getFromClause();
        // case for when no From clause is present. Creates a ProjectNode.
        if (fromClause == null) {
            plan = new ProjectNode(selClause.getSelectValues());
            plan.prepare();
            return plan;
        }
        // implementation of our ExpressionProcessor
        AggregateFinder processor = new AggregateFinder();

        List<SelectValue> selectValues = selClause.getSelectValues();
        // call helper function to recursively handle From Clause
        plan = processFromClause(fromClause, selClause, processor);
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null){
            whereExpr.traverse(processor);
            if (!processor.aggregates.isEmpty()) {
                throw new InvalidSQLException("Can't have aggregates in WHERE\n");
            }
            plan = PlanUtils.addPredicateToPlan(plan, whereExpr);
        }


        for (SelectValue sv : selectValues) {
            // Skip select-values that aren't expressions
            if (!sv.isExpression())
                continue;

            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
        }

        Map<String, FunctionCall> colMap = processor.initMap();

        List<Expression> groupByExprs = selClause.getGroupByExprs();


        if (!groupByExprs.isEmpty() || !colMap.isEmpty()){
            plan = new HashedGroupAggregateNode(plan, groupByExprs, colMap);
        }

        Expression havingExpr = selClause.getHavingExpr();
        if (havingExpr != null){
            havingExpr.traverse(processor);
            selClause.setHavingExpr(havingExpr);
            plan = PlanUtils.addPredicateToPlan(plan, havingExpr);
        }



        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (orderByExprs.size() > 0){
            // need to do something about order by clause.
            plan = new SortNode(plan, orderByExprs);
        }

        if (!selClause.isTrivialProject())
            plan = new ProjectNode(plan, selectValues);

        plan.prepare();
        return plan;
    }
}

