package edu.caltech.nanodb.queryeval;

import java.io.File;
import java.util.ArrayList;
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
 * This planner implementation allows for subqueries in <tt>SELECT</tt>,
 * <tt>WHERE</tt>,<tt>HAVING</tt> clauses.  The planner uses another
 * planner in order to plan the found subqueries recursively.
 * This passed planners implementation will then traverse using the
 * ExpressionPlanner over the subquery expression.
 */
public class ExpressionPlanner implements ExpressionProcessor {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = LogManager.getLogger(
            CostBasedJoinPlanner.class);

    /**
     * The expression planner used to create the plan for subqueries.
     */
    private Planner exprPlanner;

    /**
     * The list of Select Clauses that enclose any subquery we find.
     */
    private List<SelectClause> enclosingSelects;

    /**
     * The environment used to allow communication between subqueries.
     */
    private Environment environment;

    /**
     * A boolean flag that is true if the current expression has a subquery.
     */
    private Boolean foundSubQuery;

    /**
     * Constructs a new ExpressionPlanner.  This planner is used on the
     * entire input expression.
     *
     * @param exprPlanner the planner used to plan subqueries
     *
     * @param enclosingSelects the list of enclosing Select Clauses around
     *        any potential subqueries to be found in the expression.
     */
    public ExpressionPlanner(Planner exprPlanner, List<SelectClause> enclosingSelects) {
        this.exprPlanner = exprPlanner;
        this.enclosingSelects = enclosingSelects;
        // Sets the environment to a new empty environment
        this.environment = new Environment();
        // Initializes the foundSubQuery flag to false
        this.foundSubQuery = false;
    }

    /**
     * This helper function allows access to the ExpressionPlanners
     * environment variable.
     *
     * @return the environment of the parent query
     */
    public Environment getEnvironment() {
        return environment;
    }

    /**
     * This helper function allows for setting the environment.
     */
    public void setEnvironment(Environment passedEnvironment) {
        this.environment = passedEnvironment;
    }

    /**
     * This helper function allows access to the ExpressionPlanners
     * foundSubQuery Boolean.
     *
     * @return a boolean stating whether a subquery was found
     */
    public Boolean getFoundSubQuery() {
        return foundSubQuery;
    }

    /**
     * This method is called when expression-traversal is entering a
     * particular node in the expression tree.  It is not possible to replace
     * a node when entering it, because this would unnecessarily complicate
     * the semantics of expression-tree traversal.
     *
     * @param e the {@code Expression} node being entered
     */
    public void enter(Expression e) {
        // Check if there is a subquery
        if (e instanceof SubqueryOperator) {
            foundSubQuery = true;  // Set the foundSubQuery flag
            ArrayList<SelectClause> childSelects = new ArrayList<>();
            if (enclosingSelects != null) {
                for (SelectClause sel : enclosingSelects) {
                    childSelects.add(sel);
                }
            }

            // Generate the subquery plannode and set the subquery plan
            SubqueryOperator subquery = (SubqueryOperator) e;
            PlanNode subplan = exprPlanner.makePlan(subquery.getSubquery(), childSelects);
            subquery.setSubqueryPlan(subplan);

            // Check if it is correlated with a subquery.
            if (subquery.getSubquery().isCorrelated()) {
                // Add Parent environment if it is correlated.
                subplan.addParentEnvironmentToPlanTree(environment);
            }

        }
    }

    /**
     * This method is called when expression-traversal is leaving a particular
     * node in the expression tree.  To facilitate mutation of expression
     * trees, this method must return an {@code Expression} object:  If the
     * expression processor wants to replace the node being left with a
     * different node, this method can return the replacement node; otherwise,
     * the method should return the passed-in node.
     *
     * @param e the {@code Expression} node being left
     *
     * @return the {@code Expression} object to use for the node being left;
     *         either {@code node} if no changes are to be made, or a new
     *         {@code Expression} object if {@code node} should be replaced.
     */
    public Expression leave(Expression e) {
        // We don't wish to replace the node
        return e;
    }
}