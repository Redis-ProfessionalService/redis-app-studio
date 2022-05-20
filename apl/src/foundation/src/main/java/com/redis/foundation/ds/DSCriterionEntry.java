/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.redis.foundation.ds;

import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataItem;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Date;

/**
 * A DSCriterionEntry is responsible for encapsulating a criterion and
 * a boolean operator.  It is exclusively referenced from within a
 * {@link DSCriteria} object.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DSCriterionEntry
{
	private ArrayList<DSCriterion> mDSCriterions;
	private Data.Operator mConditionalOperator = Data.Operator.AND;

	/**
	 * Constructor accepts a {@link DSCriterion} initializes the
	 * DSCriterionEntry accordingly.
	 *
	 * @param aDSCriterion Data source criterion instance.
	 */
	public DSCriterionEntry(DSCriterion aDSCriterion)
	{
		mDSCriterions = new ArrayList<DSCriterion>();
		add(aDSCriterion);
	}

	/**
	 * Constructor accepts a logical operator and a {@link DSCriterion}
	 * and initializes the DSCriterionEntry accordingly.
	 *
	 * @param anOperator Field operator.
	 * @param aDSCriterion Data source criterion instance.
	 */
	public DSCriterionEntry(Data.Operator anOperator, DSCriterion aDSCriterion)
	{
		mDSCriterions = new ArrayList<DSCriterion>();
		setBooleanOperator(anOperator);
		add(aDSCriterion);
	}

	/**
	 * Copy constructor.
	 *
	 * @param aDSCriterionEntry Data source criterion entry instance.
	 */
	public DSCriterionEntry(final DSCriterionEntry aDSCriterionEntry)
	{
		if (aDSCriterionEntry != null)
		{
			setBooleanOperator(aDSCriterionEntry.getBooleanOperator());
			this.mDSCriterions = new ArrayList<DSCriterion>();
			aDSCriterionEntry.getCriterions().forEach(c -> {
				DSCriterion nc = new DSCriterion(c);
			});
		}
	}

	/**
	 * Returns a string summary representation of a criterion entry.
	 *
	 * @return String summary representation of a criterion  entry.
	 */
	@Override
	public String toString()
	{
		return String.format("%s [%d criterions]", mConditionalOperator.name(), mDSCriterions.size());
	}

	/**
	 * Adds the criterion instance to the criterion entry.
	 *
	 * @param aDSCriterion Data source criterion instance.
	 */
	public void add(DSCriterion aDSCriterion)
	{
		mDSCriterions.add(aDSCriterion);
	}

	/**
	 * Returns the boolean operator for the criterion entry.
	 *
	 * @return Boolean operator.
	 */
	public Data.Operator getBooleanOperator()
	{
		return mConditionalOperator;
	}

	/**
	 * Assigns a boolean operator for the criterion entry.
	 *
	 * @param anOperator Boolean operator.
	 */
	public void setBooleanOperator(Data.Operator anOperator)
	{
		mConditionalOperator = anOperator;
	}

	/**
	 * Returns the count of values associated with this criterion entry.
	 *
	 * @return Count of values.
	 */
	public int count()
	{
		return mDSCriterions.size();
	}

	/**
	 * Returns <i>true</i> if the criterion entry is simple in
	 * its nature.  A simple criteria is one where its boolean
	 * operators is <i>AND</i> and its values is single.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isSimple()
	{
		return ((mConditionalOperator == Data.Operator.AND) && (mDSCriterions.size() == 1));
	}

	/**
	 * Returns an array of criterions being managed by this
	 * criterion entry.
	 *
	 * @return An array of criterions.
	 */
	public ArrayList<DSCriterion> getCriterions()
	{
		return mDSCriterions;
	}

	/**
	 * Returns the criterion identified by the offset parameter.
	 *
	 * @param anOffset Offset into the internal criterion array.
	 *
	 * @return Data source criterion.
	 */
	public DSCriterion getCriterion(int anOffset)
	{
		return mDSCriterions.get(anOffset);
	}

	/**
	 * Convenience method that returns the first criterion within
	 * the internally managed criterion array.
	 *
	 * @return Data source criterion.
	 */
	public DSCriterion getCriterion()
	{
		return mDSCriterions.get(0);
	}

	/**
	 * Convenience method that returns the logical operator
	 * from the first criterion within the internally managed
	 * criterion array.
	 *
	 * @return Logical operator.
	 */
	public Data.Operator getLogicalOperator()
	{
		return getCriterion().getLogicalOperator();
	}

	/**
	 * Convenience method that returns the item name from the
	 * first criterion within the internally managed criterion
	 * array.
	 *
	 * @return Item name.
	 */
	public String getName()
	{
		return getCriterion().getName();
	}

	/**
	 * Convenience method that returns the item value from the
	 * first criterion within the internally managed criterion
	 * array.
	 *
	 * @return Item value.
	 */
	public String getValue()
	{
		return getCriterion().getValue();
	}

	/**
	 * Convenience method that returns the item value (formatted
	 * as a Date) from the first criterion within the internally
	 * managed criterion array.
	 *
	 * @return Item value.
	 */
	public Date getValueAsDate()
	{
		return getCriterion().getValueAsDate();
	}

	/**
	 * Returns the value associated with the criterion based on
	 * the offset parameter.
	 *
	 * @param anOffset Offset into the internal criterion array.
	 *
	 * @return Field value.
	 */
	public String getValue(int anOffset)
	{
		if (anOffset == 0)
			return getValue();
		else
		{
			DSCriterion dsCriterion = getCriterion();
			if (dsCriterion.isMultiValue())
			{
				ArrayList<String> valueList = dsCriterion.getValues();
				if (anOffset < valueList.size())
					return valueList.get(anOffset);
			}
		}

		return StringUtils.EMPTY;
	}

	/**
	 * Returns the value (formatted as a Date) associated with the
	 * criterion based on the offset parameter.
	 *
	 * @param anOffset Offset into the internal criterion array.
	 *
	 * @return Item value.
	 */
	public Date getValueAsDate(int anOffset)
	{
		if (anOffset == 0)
			return getValueAsDate();
		else
		{
			DSCriterion dsCriterion = getCriterion();
			if (dsCriterion.isMultiValue())
			{
				ArrayList<String> valueList = dsCriterion.getValues();
				if (anOffset < valueList.size())
				{
					String dateValue = valueList.get(anOffset);
					return Data.createDate(dateValue, Data.FORMAT_DATETIME_DEFAULT);
				}
			}
		}

		return new Date();
	}

	/**
	 * Convenience method that returns the {@link DataItem}
	 * from the first criterion within the internally managed
	 * criterion array.
	 *
	 * @return Simple item.
	 */
	public DataItem getItem()
	{
		return getCriterion().getItem();
	}

	/**
	 * Convenience method that returns <i>true</i> if the
	 * criterion entry is case sensitive.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isCaseInsensitive()
	{
		return getCriterion().isCaseInsensitive();
	}
}
