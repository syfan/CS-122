package edu.caltech.nanodb.plannodes;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.queryeval.PlanCost;

import edu.caltech.nanodb.relations.Tuple;


/**
 * This plan node provides a simple in-memory sort operation for use in
 * ORDER BY clauses.
 */
public class SortNode extends PlanNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = LogManager.getLogger(SortNode.class);

    /** A specification of the ordering of the results of this plan node. */
    private List<OrderByExpression> orderByExprs;

    /**
     * This array receives all tuples from the child plan node, and then they
     * are sorted and passed along to the parent from this array.
     */
    private ArrayList<Tuple> sortedResults;

    /**
     * The comparator that imposes the ordering specification of the sort node.
     */
    private TupleComparator comparator;

    /** The index of the current tuple in the sorted results. */
    private int currentTupleIndex;

    /**
     * A flag indicating whether the sort node has generate all of its output or not.
     */
    private boolean done;


    /**
     * Constructs a PlanNode with a given operation type.  This method will be
     * called by subclass constructors.
     *
     * @param subplan the subplan that produces the results to sort
     * @param orderByExprs a specification of how the results should be ordered
     */
    public SortNode(PlanNode subplan, List<OrderByExpression> orderByExprs) {
        super(subplan);

        if (orderByExprs == null)
            throw new IllegalArgumentException("orderByExprs cannot be null");

        if (orderByExprs.isEmpty()) {
            throw new IllegalArgumentException(
                "orderByExprs must include at least one expression");
        }

        this.orderByExprs = orderByExprs;
    }


    public List<OrderByExpression> resultsOrderedBy() {
        return orderByExprs;
    }


    /** The sort plan-node doesn't support marking. */
    public boolean supportsMarking() {
        return false;
    }


    /**
     * The sort plan-node doesn't require marking from either of its children.
     */
    public boolean requiresLeftMarking() {
        return false;
    }


    /**
     * The sort plan-node doesn't require marking from either of its children.
     */
    public boolean requiresRightMarking() {
        return false;
    }


    /**
     * The sort plan-node produces the same schema as its child plan-node, so
     * this method simply caches the subplan's schema object.
     */
    public void prepare() {
        // Need to prepare the left child-node before we can do our own work.
        leftChild.prepare();

        // Grab the schema and column-statistics from the left child.
        schema = leftChild.getSchema();
        stats = leftChild.getStats();

        // Grab the left child's cost, then update the cost based on the cost
        // of sorting.

        PlanCost childCost = leftChild.getCost();
        if (childCost != null) {
            cost = new PlanCost(childCost);

            // Sorting in memory is an N*log(N) operation.
            cost.cpuCost += cost.numTuples * (float) Math.log(cost.numTuples);
        }
        else {
            logger.info(
                "Child's cost not available; not computing this node's cost.");
        }

        // We can prepare the tuple-comparator here too, since we know what the
        // subplan's schema will be.
        comparator = new TupleComparator(schema, orderByExprs);
    }


    /**
     * Does any initialization the node might need.  This could include
     * resetting state variables or starting the node over from the beginning.
     *
     */
    public void initialize() {
        super.initialize();

        sortedResults = null;
        done = false;

        leftChild.initialize();
    }


    /**
     * Gets the next tuple that fulfills the conditions for this plan node.
     * If the node has a child, it should call getNextTuple() on the child.
     * If the node is a leaf, the tuple comes from some external source such
     * as a table file, the network, etc.
     *
     * @return the next tuple to be generated by this plan, or <tt>null</tt>
     *         if the plan has finished generating plan nodes.
     *
     * @throws IllegalStateException if a plan node is not properly initialized
     */
    public Tuple getNextTuple() throws IllegalStateException {
        if (done)
            return null;

        if (sortedResults == null)
            prepareSortedResults();

        Tuple tup = null;
        if (currentTupleIndex < sortedResults.size()) {
            tup = sortedResults.get(currentTupleIndex);
            currentTupleIndex++;
        }
        else {
            done = true;
        }

        return tup;
    }


    private void prepareSortedResults() {
        sortedResults = new ArrayList<>();
        while (true) {
            // Get the next tuple.  If it's not cacheable then make a copy
            // of it before storing it away.  (This is cheating; we are
            // allowing the backing data buffers to be reclaimed by storing
            // the tuple data outside of the Buffer Manager's buffers.)

            Tuple tup = leftChild.getNextTuple();
            if (tup == null)
                break;

            if (tup.isDiskBacked()) {
                Tuple copy = TupleLiteral.fromTuple(tup);
                tup.unpin();
                tup = copy;
            }

            sortedResults.add(tup);
        }

        Collections.sort(sortedResults, comparator);

        currentTupleIndex = 0;
    }


    /** Clean up after evaluation of the sort plan-node. */
    public void cleanUp() {
        // Allow this collection to be garbage-collected.
        sortedResults = null;

        leftChild.cleanUp();
    }


    @Override
    public String toString() {
        return "Sort[" + orderByExprs + "]";
    }


    /**
     * Checks if the argument is a plan node tree with the same structure,
     * but not necesarily the same references.
     *
     * @param obj the object to which we are comparing
     *
     * @design We re-declare this here to force its implementation in subclasses.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SortNode) {
            SortNode other = (SortNode) obj;

            return orderByExprs.equals(other.orderByExprs) &&
                   leftChild.equals(other.leftChild);
        }
        return false;
    }


    /**
     * Computes the hash-code of a plan-node, including any sub-plans of this
     * plan.  This method is used to see if two plan nodes (or subtrees)
     * <em>might be</em> equal.
     *
     * @return the hash code for the plan node and any subnodes it may contain.
     */
    @Override
    public int hashCode() {
        int hash = 17;
        hash = 31 * hash + orderByExprs.hashCode();
        hash = 31 * hash + leftChild.hashCode();
        return hash;
    }
}
