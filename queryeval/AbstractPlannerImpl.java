package edu.caltech.nanodb.queryeval;


import java.util.*;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.plannodes.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;

/**
 * Contains code that is common across query planners.
 */
public abstract class AbstractPlannerImpl implements Planner{
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = LogManager.getLogger(AbstractPlannerImpl.class);

    /** The storage manager used during query planning. */
    protected StorageManager storageManager;


    /** Sets the server to be used during query planning. */
    public void setStorageManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
    }

    public static class AggregateFinder implements ExpressionProcessor {
        // keep track of the aggregate function calls
        public List<FunctionCall> aggregates = new ArrayList<>();
        // keep track of the calls themselves
        public List<FunctionCall> functionCalls = new ArrayList<>();
        public Map<String, FunctionCall> mapStrToFunc = new HashMap<>();
        public Set<String> colNames = new HashSet<>();
        public int aggCount = 0;

        public Map<String, FunctionCall> initMap() {
            Map<String, FunctionCall> colMap = new HashMap<>();
            // setting up the initial hashes for the map
            for (int i = 0; i < functionCalls.size(); i++) {
                colMap.put("#A" + (i+1), functionCalls.get(i));
            }
            return colMap;
        }


        public void enter(Expression e) {
            if (e instanceof FunctionCall) {
                FunctionCall call = (FunctionCall) e;
                Function f = call.getFunction();
                if (f instanceof AggregateFunction) {
                    if (!aggregates.isEmpty()) {
                        throw new IllegalArgumentException("Aggregate function call cannot be an argument");
                    }
                    if (!functionCalls.contains(call)) {
                        // add the aggregate call to our members
                        aggCount += 1;
                        String colName = "#A" + (aggCount);
                        mapStrToFunc.put(colName, call);
                        functionCalls.add(call);
                        aggregates.add(call);

                    }
                }
            }
        }

        public Expression leave(Expression e) {
            if (e instanceof FunctionCall) {
                FunctionCall call = (FunctionCall) e;
                Function f = call.getFunction();
                // replace the e FunctionCall with a columne reference by using
                // the ColumnValue class
                if (f instanceof AggregateFunction) {
                    String colName = "#A" + (aggCount);
                    // The call for the functionCall has not been found
                    if (!colNames.contains(colName)) {
                        colNames.add(colName);
                        ColumnName tempName = new ColumnName(colName);
                        e = new ColumnValue(tempName);
                        if (!aggregates.isEmpty()) {
                            aggregates.remove(aggregates.size() - 1);
                        }

                    }
                    else if (mapStrToFunc.containsKey(colName)) {
                        ColumnName tempName = new ColumnName(colName);
                        e = new ColumnValue(tempName);
                    }
                }
            }
            return e;
        }
    }

    public PlanNode processFromClause(FromClause fromClause, SelectClause selClause,
                                      AggregateFinder processor) {
        PlanNode plan = null;

        if (fromClause.isBaseTable()) {
            plan = makeSimpleSelect(fromClause.getTableName(),
                    null, null);
        }
        else if (fromClause.isDerivedTable()){
            plan = makePlan(fromClause.getSelectClause(), null);
        }
        else if (fromClause.isJoinExpr()){
            Expression onExpr = fromClause.getOnExpression();
            if (onExpr != null) {
                onExpr.traverse(processor);
                if(!processor.aggregates.isEmpty()){
                    throw new InvalidSQLException("Cannot have aggregates in ON\n");
                }
            }
            FromClause leftFrom = fromClause.getLeftChild();
            FromClause rightFrom = fromClause.getRightChild();

            PlanNode leftChild = processFromClause(leftFrom, selClause, processor);
            PlanNode rightChild = processFromClause(rightFrom, selClause, processor);
            plan = new NestedLoopJoinNode(leftChild, rightChild, fromClause.getJoinType(),
                    fromClause.getOnExpression());
        }
        if(fromClause.isRenamed()){
            plan = new RenameNode(plan, fromClause.getResultName());
        }

        plan.prepare();
        return plan;
    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
                                       List<SelectClause> enclosingSelects) {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                    "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}
