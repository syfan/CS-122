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
import org.antlr.v4.misc.EscapeSequenceParsing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.ColumnInfo;

/**
 * This class implements ExpressionProcessor in order to allow
 * entry into subqueries.  Its purpose is to compute a cost for
 * an expression with subqueries in <tt>SELECT</tt>-<tt>WHERE</tt>
 * or <tt>HAVING</tt>
 */
public class ExpressionCostCalculator implements ExpressionProcessor {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = LogManager.getLogger(
            CostBasedJoinPlanner.class);

    /**
     * The float cost object that will be computed via traversing
     * through the ExpressionCostCalculator.
     */
    private float cost;

    /**
     * The constructor.  Set's the initial cost of the expression to 0.
     */
    public ExpressionCostCalculator() {
        this.cost = 0;
    }

    /**
     * This helper function allows access to the calculators cost field.
     * @return the cost calculated by the cost calculator.
     */
    public float getCost() {
        return this.cost;
    }
    public void enter(Expression e) {
        this.cost += 1;
        if (e instanceof SubqueryOperator) {
            SubqueryOperator subquery = (SubqueryOperator) e;
            List<SelectValue> selectValues = subquery.getSubquery().getSelectValues();
            for (SelectValue sv : selectValues) {
                // Skip select-values that aren't expressions
                if (!sv.isExpression())
                    continue;

                // Enter any expressions of the subquery selectvalues
                Expression exp = sv.getExpression();
                enter(exp);
            }
            // Add the cpuCost of the subquery plan.
            this.cost += subquery.getSubqueryPlan().getCost().cpuCost;
        }
    }

    public Expression leave(Expression e) {
        return e;
    }
}