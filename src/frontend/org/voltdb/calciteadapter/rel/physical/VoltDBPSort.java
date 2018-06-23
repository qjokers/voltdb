/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calciteadapter.rel.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.voltdb.calciteadapter.converter.RelConverter;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.types.SortDirectionType;

public class VoltDBPSort extends Sort implements VoltDBPRel {

    private final int m_splitCount;

    public VoltDBPSort(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation,
            int splitCount) {
        this(cluster, traitSet, input, collation, null, null, splitCount);
    }

    private VoltDBPSort(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode limit,
            int splitCount) {
        super(cluster, traitSet, input, collation, offset, limit);
        assert traitSet.contains(VoltDBPRel.VOLTDB_PHYSICAL);
        m_splitCount = splitCount;
    }

    @Override
    public VoltDBPSort copy(RelTraitSet traitSet,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode limit) {
        return new VoltDBPSort(
                getCluster(),
                traitSet,
                input,
                collation,
                offset,
                limit,
                m_splitCount);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        AbstractPlanNode child = this.inputRelNodeToPlanNode(this, 0);

        LimitPlanNode lpn = null;
        if (fetch != null || offset != null) {
            lpn = new LimitPlanNode();
            if (fetch != null) {
                lpn.setLimit(RexLiteral.intValue(fetch));
            }
            if (offset != null) {
                lpn.setOffset(RexLiteral.intValue(offset));
            }
        }
        OrderByPlanNode opn = null;
        RelCollation collation = getCollation();
        if (collation != null) {
            opn = new OrderByPlanNode();

            // Convert ORDER BY Calcite expressions to VoltDB
            List<AbstractExpression> voltExprList = new ArrayList<>();
            for (RexNode expr : fieldExps) {
                AbstractExpression voltExpr = RexConverter.convert(expr);
                voltExprList.add(voltExpr);
            }
            List<Pair<Integer, SortDirectionType>> collFields = RelConverter.convertCollation(collation);
            assert(voltExprList.size() == collFields.size());
            int index = 0;
            for (Pair<Integer, SortDirectionType> collField : collFields) {
                opn.getSortExpressions().add(voltExprList.get(index++));
                opn.getSortDirections().add(collField.right);
            }
        }
        AbstractPlanNode result = null;
        if (opn != null) {
            opn.addAndLinkChild(child);
            result = opn;
            if (lpn != null) {
                opn.addInlinePlanNode(lpn);
            }
        } else {
            assert(lpn != null);
            lpn.addAndLinkChild(child);
            result = lpn;
        }
        return result;
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        digest += "_split_" + m_splitCount;
        return digest;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("split", m_splitCount);
        return pw;
    }

}