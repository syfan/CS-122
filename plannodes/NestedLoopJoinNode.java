package edu.caltech.nanodb.plannodes;


import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.queryeval.StatisticsUpdater;
import edu.caltech.nanodb.relations.Schema;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.PlanCost;
import edu.caltech.nanodb.queryeval.SelectivityEstimator;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This plan node implements a nested-loop join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopJoinNode extends ThetaJoinNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = LogManager.getLogger(NestedLoopJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;

    /** Extra tuple for storing data*/
    private Tuple outerTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;

    /** Set to true when we have found a pair of tuples that can be joined.
     * It's purpose is to determine whether a rwo needs to be adder for
     * outer joins. */
    private boolean match_found;

    /** Null value flag.  This flag is set when one of the tuples added
     * is all nulls as it is created for an outer join.  */
    private boolean null_flag;


    public NestedLoopJoinNode(PlanNode leftChild, PlanNode rightChild,
                              JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopJoinNode) {
            NestedLoopJoinNode other = (NestedLoopJoinNode) obj;

            return predicate.equals(other.predicate) &&
                leftChild.equals(other.leftChild) &&
                rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the nested-loop plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoop[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopJoinNode node = (NestedLoopJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();

        rightChild.prepare();

        // Use the parent class' helper-function to prepare the schema.
        // This prepares the stats too
        prepareSchemaStats();

        // Initialize cost estimate to that of the left child
        cost = new PlanCost(leftChild.getCost());
        // Update estimates for tuples size by adding the average tuples size
        // of the two children.
        cost.tupleSize += rightChild.getCost().tupleSize;
        // Update estimates for cpuCost by adding the right child's cpu cost
        // and then adding the product of each child's number of tuples.
        // This is due to the fact that to compute the join, the cpu
        // compares every combination of tuples.
        cost.cpuCost += rightChild.getCost().cpuCost;
        cost.cpuCost += leftChild.getCost().numTuples * rightChild.getCost().numTuples;
        // The numBlockIO and numLargeSeeks are updated by adding the right
        // child's value multiplied by the left child's number of tuples
        // to the initialized left child's value.
        cost.numBlockIOs += rightChild.getCost().numBlockIOs * leftChild.getCost().numTuples;
        cost.numLargeSeeks += rightChild.getCost().numLargeSeeks * leftChild.getCost().numTuples;

        // The number of tuples estimate depends on the join type
        float sigma = 0;
        switch (joinType) {
            case CROSS:
                // We simply SELECT every combination of tuples
                cost.numTuples *= rightChild.getCost().numTuples;
                break;

            case INNER:
                // A subset of every combination of tuples are returned
                cost.numTuples *= rightChild.getCost().numTuples;
                // If there are no tuples, we can't remove any by selection.
                if (cost.numTuples == 0) {
                    break;
                }
                sigma = SelectivityEstimator.estimateSelectivity(predicate, schema, stats);
                cost.numTuples *= sigma;
                break;

            case LEFT_OUTER:
                // We first SELECT every combination of tuples
                cost.numTuples *= rightChild.getCost().numTuples;
                sigma = SelectivityEstimator.estimateSelectivity(predicate, schema, stats);
                cost.numTuples *= sigma;
                // Now estimate the number of added tuples required such that
                // Every left tuple is in the output.
                cost.numTuples += leftChild.getCost().numTuples *
                        Math.pow((1 - sigma), rightChild.getCost().numTuples);
                break;

            case RIGHT_OUTER:
                // We first SELECT every combination of tuples
                cost.numTuples *= rightChild.getCost().numTuples;
                sigma = SelectivityEstimator.estimateSelectivity(predicate, schema, stats);
                cost.numTuples *= sigma;
                // Now estimate the number of added tuples required such that
                // Every right tuple is in the output.
                cost.numTuples += rightChild.getCost().numTuples *
                        Math.pow((1 - sigma), leftChild.getCost().numTuples);
                break;

            case FULL_OUTER:
                // We first SELECT every combination of tuples
                cost.numTuples *= rightChild.getCost().numTuples;
                sigma = SelectivityEstimator.estimateSelectivity(predicate, schema, stats);
                cost.numTuples *= sigma;
                // Now estimate the number of added tuples required such that
                // Every left and right tuple are in the output.
                cost.numTuples += rightChild.getCost().numTuples *
                        Math.pow((1 - sigma), leftChild.getCost().numTuples);
                cost.numTuples += leftChild.getCost().numTuples *
                        Math.pow((1 - sigma), rightChild.getCost().numTuples);
                break;

            default:
                assert false : "Unexpected Join type:  " + joinType;
        }

        // Update the stats
        stats = StatisticsUpdater.updateStats(predicate, schema, stats);
    }


    public void initialize() {
        super.initialize();

        // Initialize flags
        done = false;
        match_found = false;
        null_flag = false;

        // Initialize tuples
        rightTuple = null;
        // swap order of right and left children when right outer join is desired.
        if (joinType == JoinType.RIGHT_OUTER) {
            swap();
        }
        leftTuple = leftChild.getNextTuple();
    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     */
    public Tuple getNextTuple() {
        if (done)
            return null;

        while (getTuplesToJoin()) {
            // When one terminal is filled with nulls, accept it
            if (null_flag) {
                null_flag = false;
                return joinTuples(leftTuple, outerTuple);
            }
            // Otherwise check it it should be accepted.
            if (canJoinTuples()) {
                match_found = true;
                return joinTuples(leftTuple, outerTuple);
            }
        }

        return null;
    }


    /**
     * This helper function implements the logic that sets {@link #leftTuple}
     * and {@link #rightTuple} based on the nested-loops logic.
     *
     * @return {@code true} if another pair of tuples was found to join, or
     *         {@code false} if no more pairs of tuples are available to join.
     */
    private boolean getTuplesToJoin() {
        // Get the next tuple
        rightTuple = rightChild.getNextTuple();
        if (rightTuple == null) {
            if (!match_found && (joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER)) {
                /* If the tuple is null and an outer join requires addition of the lefttuple to the output
                 * then create a TupleLiteral, set the null_flag, set the match_found flag and return */
                outerTuple = TupleLiteral.ofSize(rightSchema.numColumns());
                rightTuple = outerTuple;
                null_flag = true;
                if (leftTuple == null) {  //  Verify that the lefttuple exists
                    done = true;
                    return false;
                }
                match_found = true;
                return true;
            }
            // No more tuples need to be considered
            if (leftTuple == null) {
                done = true;
                return false;
            }
            // Reset the inner loop of the right tuple and get the next left tuple resetting the match_found flag.
            rightChild.initialize();
            leftTuple = leftChild.getNextTuple();
            match_found = false;
            rightTuple = rightChild.getNextTuple();
            outerTuple = rightTuple;
            // If the right tuple is still null after the update, make sure a TupleLiteral is created.
            if (rightTuple == null) {
                outerTuple = TupleLiteral.ofSize(rightSchema.numColumns());
                rightTuple = outerTuple;
            }
        }
        else {
            outerTuple = rightTuple;
        }
        if (leftTuple == null) {
            done = true;
            return false;
        }
        return true;
    }


    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
        environment.addTuple(rightSchema, rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
        //        (Just haven't gotten around to implementing this.)
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
