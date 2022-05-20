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

package com.redis.ds.ds_grid;

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterionEntry;
import com.redis.foundation.ds.DSException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * This is a helper class for the Grid data source and should not
 * be invoked by applications.  Its purpose is to build and execute
 * queries against in-memory grids.
 */
public class GridCriteria
{
	private int mLimit;
	private int mOffset;
	Criterion mCriteria;
	private final AppCtx mAppCtx;
	private final DataDoc mSchema;

	public GridCriteria(AppCtx anAppCtx, DataDoc aSchema)
	{
		mSchema = aSchema;
		mAppCtx = anAppCtx;
		mLimit = DS.CRITERIA_QUERY_LIMIT_DEFAULT;
		mOffset = DS.CRITERIA_QUERY_OFFSET_DEFAULT;
	}

	@FunctionalInterface
	private interface Criterion
	{
		Stream<DataDoc> apply(Stream<DataDoc> aStream);
	}

	/**
	 * Create a criterion from a predicate.
	 *
	 * @param aDDPredicate Data document predicate instance
	 *
	 * @return Criterion instance
	 */
	private Criterion criterionPredicate(Predicate<DataDoc> aDDPredicate)
	{
		return ddStream -> ddStream.filter(aDDPredicate);
	}

	private Criterion criterionWithOffset(long anOffset)
	{
		return ddStream -> ddStream.skip(anOffset);
	}

	private Criterion criterionWithLimit(long aLimit)
	{
		return ddStream -> ddStream.limit(aLimit);
	}

	private Criterion criterionWithOffsetLimit(long anOffset, long aLimit)
	{
		return ddStream -> ddStream.skip(anOffset).limit(aLimit);
	}

	private Criterion criterionSortAscending(Comparator<DataDoc> aComparator)
	{
		return ddStream -> ddStream.sorted(aComparator);
	}

	private Criterion criterionSortDescending(Comparator<DataDoc> aComparator)
	{
		return ddStream -> ddStream.sorted(aComparator.reversed());
	}

	private Criterion criterionWithOffsetSort(Comparator<DataDoc> aComparator, long anOffset)
	{
		return ddStream -> ddStream.skip(anOffset).sorted(aComparator);
	}

	private Criterion criterionSortWithLimit(Comparator<DataDoc> aComparator, long aLimit)
	{
		return ddStream -> ddStream.sorted(aComparator).limit(aLimit);
	}

	private Criterion criterionWithOffsetSortLimit(Comparator<DataDoc> aComparator, long anOffset, long aLimit)
	{
		return ddStream -> ddStream.skip(anOffset).sorted(aComparator).limit(aLimit);
	}

	private boolean isNotEmpty(String aValue)
	{
		return StringUtils.isNotEmpty(aValue);
	}

	private Optional<Criterion> booleanCriterion(DSCriterionEntry aCE)
	{
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "booleanCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Criterion criterion = null;
		DataItem dataItem = aCE.getItem();
		boolean ceValue = dataItem.getValueAsBoolean();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				criterion = criterionPredicate(dd -> dd.getValueAsBoolean(dataItem.getName()) == ceValue);
				break;
			case NOT_EQUAL:
				criterion = criterionPredicate(dd -> dd.getValueAsBoolean(dataItem.getName()) != ceValue);
				break;
			case EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsBoolean(dataItem.getName()) == dd.getValueAsBoolean(itemName));
				break;
			case NOT_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsBoolean(dataItem.getName()) != dd.getValueAsBoolean(itemName));
				break;
			case EMPTY:
				criterion = criterionPredicate(dd -> !dd.isValueAssigned(dataItem.getName()));
				break;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(criterion);
	}

	private Optional<Criterion> integerCriterion(DSCriterionEntry aCE)
	{
		int cdValue1;
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "integerCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Criterion criterion = null;
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				cdValue1 = dataItem.getValueAsInteger();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) == cdValue1);
				break;
			case NOT_EQUAL:
				cdValue1 = dataItem.getValueAsInteger();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) != cdValue1);
				break;
			case GREATER_THAN:
				cdValue1 = dataItem.getValueAsInteger();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) > cdValue1);
				break;
			case STARTS_WITH:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsInteger();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) >= cdValue1);
				break;
			case LESS_THAN:
				cdValue1 = dataItem.getValueAsInteger();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) < cdValue1);
				break;
			case LESS_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsInteger();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) <= cdValue1);
				break;
			case EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) == dd.getValueAsInteger(itemName));
				break;
			case NOT_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) != dd.getValueAsInteger(itemName));
				break;
			case GREATER_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) > dd.getValueAsInteger(itemName));
				break;
			case STARTS_WITH_FIELD:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) >= dd.getValueAsInteger(itemName));
				break;
			case LESS_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) < dd.getValueAsInteger(itemName));
				break;
			case LESS_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsInteger(dataItem.getName()) <= dd.getValueAsInteger(itemName));
				break;
			case BETWEEN:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsInteger();
					int ceValue2 = Data.createInt(dataItem.getValues().get(1));
					criterion = criterionPredicate(dd -> ((dd.getValueAsInteger(dataItem.getName()) > cdValue1) &&
														  (dd.getValueAsInteger(dataItem.getName()) < ceValue2)));
				}
				break;
			case BETWEEN_INCLUSIVE:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsInteger();
					int ceValue2 = Data.createInt(dataItem.getValues().get(1));
					criterion = criterionPredicate(dd -> ((dd.getValueAsInteger(dataItem.getName()) >= cdValue1) &&
														  (dd.getValueAsInteger(dataItem.getName()) <= ceValue2)));
				}
				break;
			case IN:
				criterion = criterionPredicate(dd -> dd.isValueInByName(dataItem.getName(), dataItem.getValue()));
				break;
			case EMPTY:
				criterion = criterionPredicate(dd -> !dd.isValueAssigned(dataItem.getName()));
				break;
			case SORT:
				Data.Order sortOrder = Data.Order.valueOf(dataItem.getValue());
				if (sortOrder == Data.Order.ASCENDING)
					criterion = criterionSortAscending(comparing(dd -> dd.getValueAsInteger(dataItem.getName())));
				else
					criterion = criterionSortDescending(comparing(dd -> dd.getValueAsInteger(dataItem.getName())));
				break;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(criterion);
	}

	private Optional<Criterion> longCriterion(DSCriterionEntry aCE)
	{
		long cdValue1;
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "longCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Criterion criterion = null;
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				cdValue1 = dataItem.getValueAsLong();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) == cdValue1);
				break;
			case NOT_EQUAL:
				cdValue1 = dataItem.getValueAsLong();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) != cdValue1);
				break;
			case GREATER_THAN:
				cdValue1 = dataItem.getValueAsLong();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) > cdValue1);
				break;
			case STARTS_WITH:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsLong();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) >= cdValue1);
				break;
			case LESS_THAN:
				cdValue1 = dataItem.getValueAsLong();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) < cdValue1);
				break;
			case LESS_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsLong();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) <= cdValue1);
				break;
			case EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) == dd.getValueAsLong(itemName));
				break;
			case NOT_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) != dd.getValueAsLong(itemName));
				break;
			case GREATER_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) > dd.getValueAsLong(itemName));
				break;
			case STARTS_WITH_FIELD:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) >= dd.getValueAsLong(itemName));
				break;
			case LESS_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) < dd.getValueAsLong(itemName));
				break;
			case LESS_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsLong(dataItem.getName()) <= dd.getValueAsLong(itemName));
				break;
			case BETWEEN:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsLong();
					long ceValue2 = Data.createLong(dataItem.getValues().get(1));
					criterion = criterionPredicate(dd -> ((dd.getValueAsLong(dataItem.getName()) > cdValue1) &&
														  (dd.getValueAsLong(dataItem.getName()) < ceValue2)));
				}
				break;
			case BETWEEN_INCLUSIVE:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsLong();
					long ceValue2 = Data.createLong(dataItem.getValues().get(1));
					criterion = criterionPredicate(dd -> ((dd.getValueAsLong(dataItem.getName()) >= cdValue1) &&
														  (dd.getValueAsLong(dataItem.getName()) <= ceValue2)));
				}
				break;
			case IN:
				criterion = criterionPredicate(dd -> dd.isValueInByName(dataItem.getName(), dataItem.getValue()));
				break;
			case EMPTY:
				criterion = criterionPredicate(dd -> !dd.isValueAssigned(dataItem.getName()));
				break;
			case SORT:
				Data.Order sortOrder = Data.Order.valueOf(dataItem.getValue());
				if (sortOrder == Data.Order.ASCENDING)
					criterion = criterionSortAscending(comparing(dd -> dd.getValueAsLong(dataItem.getName())));
				else
					criterion = criterionSortDescending(comparing(dd -> dd.getValueAsLong(dataItem.getName())));
				break;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(criterion);
	}

	private Optional<Criterion> floatCriterion(DSCriterionEntry aCE)
	{
		float cdValue1;
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "floatCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Criterion criterion = null;
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				cdValue1 = dataItem.getValueAsFloat();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) == cdValue1);
				break;
			case NOT_EQUAL:
				cdValue1 = dataItem.getValueAsFloat();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) != cdValue1);
				break;
			case GREATER_THAN:
				cdValue1 = dataItem.getValueAsFloat();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) > cdValue1);
				break;
			case STARTS_WITH:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsFloat();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) >= cdValue1);
				break;
			case LESS_THAN:
				cdValue1 = dataItem.getValueAsFloat();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) < cdValue1);
				break;
			case LESS_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsFloat();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) <= cdValue1);
				break;
			case EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) == dd.getValueAsFloat(itemName));
				break;
			case NOT_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) != dd.getValueAsFloat(itemName));
				break;
			case GREATER_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) > dd.getValueAsFloat(itemName));
				break;
			case STARTS_WITH_FIELD:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) >= dd.getValueAsFloat(itemName));
				break;
			case LESS_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) < dd.getValueAsFloat(itemName));
				break;
			case LESS_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsFloat(dataItem.getName()) <= dd.getValueAsFloat(itemName));
				break;
			case BETWEEN:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsFloat();
					float ceValue2 = Data.createFloat(dataItem.getValues().get(1));
					criterion = criterionPredicate(dd -> ((dd.getValueAsFloat(dataItem.getName()) > cdValue1) &&
														  (dd.getValueAsFloat(dataItem.getName()) < ceValue2)));
				}
				break;
			case BETWEEN_INCLUSIVE:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsFloat();
					float ceValue2 = Data.createFloat(dataItem.getValues().get(1));
					criterion = criterionPredicate(dd -> ((dd.getValueAsFloat(dataItem.getName()) >= cdValue1) &&
														  (dd.getValueAsFloat(dataItem.getName()) <= ceValue2)));
				}
				break;
			case IN:
				criterion = criterionPredicate(dd -> dd.isValueInByName(dataItem.getName(), dataItem.getValue()));
				break;
			case EMPTY:
				criterion = criterionPredicate(dd -> !dd.isValueAssigned(dataItem.getName()));
				break;
			case SORT:
				Data.Order sortOrder = Data.Order.valueOf(dataItem.getValue());
				if (sortOrder == Data.Order.ASCENDING)
					criterion = criterionSortAscending(comparing(dd -> dd.getValueAsFloat(dataItem.getName())));
				else
					criterion = criterionSortDescending(comparing(dd -> dd.getValueAsFloat(dataItem.getName())));
				break;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(criterion);
	}

	private Optional<Criterion> doubleCriterion(DSCriterionEntry aCE)
	{
		double cdValue1;
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "doubleCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Criterion criterion = null;
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				cdValue1 = dataItem.getValueAsDouble();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) == cdValue1);
				break;
			case NOT_EQUAL:
				cdValue1 = dataItem.getValueAsDouble();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) != cdValue1);
				break;
			case GREATER_THAN:
				cdValue1 = dataItem.getValueAsDouble();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) > cdValue1);
				break;
			case STARTS_WITH:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsDouble();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) >= cdValue1);
				break;
			case LESS_THAN:
				cdValue1 = dataItem.getValueAsDouble();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) < cdValue1);
				break;
			case LESS_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsDouble();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) <= cdValue1);
				break;
			case EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) == dd.getValueAsDouble(itemName));
				break;
			case NOT_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) != dd.getValueAsDouble(itemName));
				break;
			case GREATER_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) > dd.getValueAsDouble(itemName));
				break;
			case STARTS_WITH_FIELD:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) >= dd.getValueAsDouble(itemName));
				break;
			case LESS_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) < dd.getValueAsDouble(itemName));
				break;
			case LESS_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				cdValue1 = dataItem.getValueAsDouble();
				criterion = criterionPredicate(dd -> dd.getValueAsDouble(dataItem.getName()) <= cdValue1);
				break;
			case BETWEEN:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsDouble();
					double ceValue2 = Data.createDouble(dataItem.getValues().get(1));
					criterion = criterionPredicate(dd -> ((dd.getValueAsDouble(dataItem.getName()) > cdValue1) &&
														  (dd.getValueAsDouble(dataItem.getName()) < ceValue2)));
				}
				break;
			case BETWEEN_INCLUSIVE:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsDouble();
					double ceValue2 = Data.createDouble(dataItem.getValues().get(1));
					criterion = criterionPredicate(dd -> ((dd.getValueAsDouble(dataItem.getName()) >= cdValue1) &&
														  (dd.getValueAsDouble(dataItem.getName()) <= ceValue2)));
				}
				break;
			case IN:
				criterion = criterionPredicate(dd -> dd.isValueInByName(dataItem.getName(), dataItem.getValue()));
				break;
			case EMPTY:
				criterion = criterionPredicate(dd -> !dd.isValueAssigned(dataItem.getName()));
				break;
			case SORT:
				Data.Order sortOrder = Data.Order.valueOf(dataItem.getValue());
				if (sortOrder == Data.Order.ASCENDING)
					criterion = criterionSortAscending(comparing(dd -> dd.getValueAsDouble(dataItem.getName())));
				else
					criterion = criterionSortDescending(comparing(dd -> dd.getValueAsDouble(dataItem.getName())));
				break;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(criterion);
	}

	private Optional<Criterion> dateCriterion(DSCriterionEntry aCE)
	{
		long cdValue1;
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "dateCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Criterion criterion = null;
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				cdValue1 = dataItem.getValueAsDate().getTime();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() == cdValue1);
				break;
			case NOT_EQUAL:
				cdValue1 = dataItem.getValueAsDate().getTime();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() != cdValue1);
				break;
			case GREATER_THAN:
				cdValue1 = dataItem.getValueAsDate().getTime();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() > cdValue1);
				break;
			case STARTS_WITH:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsDate().getTime();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() >= cdValue1);
				break;
			case LESS_THAN:
				cdValue1 = dataItem.getValueAsDate().getTime();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() < cdValue1);
				break;
			case LESS_THAN_EQUAL:
				cdValue1 = dataItem.getValueAsDate().getTime();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() <= cdValue1);
				break;
			case EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() == dd.getValueAsDate(itemName).getTime());
				break;
			case NOT_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() != dd.getValueAsDate(itemName).getTime());
				break;
			case GREATER_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() > dd.getValueAsDate(itemName).getTime());
				break;
			case STARTS_WITH_FIELD:			// handles UI grid column filtering default for numbers
			case GREATER_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() >= dd.getValueAsDate(itemName).getTime());
				break;
			case LESS_THAN_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() < dd.getValueAsDate(itemName).getTime());
				break;
			case LESS_THAN_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueAsDate(dataItem.getName()).getTime() <= dd.getValueAsDate(itemName).getTime());
				break;
			case BETWEEN:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsDate().getTime();
					ArrayList<Date> ceValueList = dataItem.getValuesAsDate();
					long ceValue2 = ceValueList.get(1).getTime();
					criterion = criterionPredicate(dd -> ((dd.getValueAsDate(dataItem.getName()).getTime() > cdValue1) &&
														  (dd.getValueAsDate(dataItem.getName()).getTime() < ceValue2)));
				}
				break;
			case BETWEEN_INCLUSIVE:
				if (dataItem.isMultiValue())
				{
					cdValue1 = dataItem.getValueAsDate().getTime();
					ArrayList<Date> ceValueList = dataItem.getValuesAsDate();
					long ceValue2 = ceValueList.get(1).getTime();
					criterion = criterionPredicate(dd -> ((dd.getValueAsDate(dataItem.getName()).getTime() >= cdValue1) &&
														  (dd.getValueAsDate(dataItem.getName()).getTime() <= ceValue2)));
				}
				break;
			case IN:
				criterion = criterionPredicate(dd -> dd.isValueInByName(dataItem.getName(), dataItem.getValue()));
				break;
			case EMPTY:
				criterion = criterionPredicate(dd -> !dd.isValueAssigned(dataItem.getName()));
				break;
			case SORT:
				Data.Order sortOrder = Data.Order.valueOf(dataItem.getValue());
				if (sortOrder == Data.Order.ASCENDING)
					criterion = criterionSortAscending(comparing(dd -> dd.getValueAsDate(dataItem.getName()).getTime()));
				else
					criterion = criterionSortDescending(comparing(dd -> dd.getValueAsDate(dataItem.getName()).getTime()));
				break;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(criterion);
	}

	private Optional<Criterion> stringSensitiveCriterion(DSCriterionEntry aCE)
	{
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "stringSensitiveCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Criterion criterion = null;
		DataItem dataItem = aCE.getItem();
		String ceValue = dataItem.getValue();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).equals(ceValue));
				break;
			case NOT_EQUAL:
				criterion = criterionPredicate(dd -> !dd.getValueByName(dataItem.getName()).equals(ceValue));
				break;
			case NOT_EMPTY:
				criterion = criterionPredicate(dd -> dd.isValueAssigned(dataItem.getName()));
				break;
			case CONTAINS:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).contains(ceValue));
				break;
			case STARTS_WITH:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).startsWith(ceValue));
				break;
			case ENDS_WITH:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).endsWith(ceValue));
				break;
			case EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).equals(dd.getValueByName(itemName)));
				break;
			case NOT_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> !dd.getValueByName(dataItem.getName()).equals(dd.getValueByName(itemName)));
				break;
			case CONTAINS_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).contains(dd.getValueByName(itemName)));
				break;
			case STARTS_WITH_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).startsWith(dd.getValueByName(itemName)));
				break;
			case ENDS_WITH_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).endsWith(dd.getValueByName(itemName)));
				break;
			case IN:
				criterion = criterionPredicate(dd -> dd.isValueInByName(dataItem.getName(), dataItem.getValues()));
				break;
			case EMPTY:
				criterion = criterionPredicate(dd -> !dd.isValueAssigned(dataItem.getName()));
				break;
			case REGEX:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).matches(ceValue));
				break;
			case SORT:
				Data.Order sortOrder = Data.Order.valueOf(dataItem.getValue());
				if (sortOrder == Data.Order.ASCENDING)
					criterion = criterionSortAscending(comparing(dd -> dd.getValueByName(dataItem.getName())));
				else
					criterion = criterionSortDescending(comparing(dd -> dd.getValueByName(dataItem.getName())));
				break;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(criterion);
	}

	private Optional<Criterion> stringInsensitiveCriterion(DSCriterionEntry aCE)
	{
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "stringInsensitiveCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Criterion criterion = null;
		DataItem dataItem = aCE.getItem();
		String ceValue = dataItem.getValue().toLowerCase();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).toLowerCase().equals(ceValue));
				break;
			case NOT_EQUAL:
				criterion = criterionPredicate(dd -> !dd.getValueByName(dataItem.getName()).toLowerCase().equals(ceValue));
				break;
			case NOT_EMPTY:
				criterion = criterionPredicate(dd -> dd.isValueAssigned(dataItem.getName()));
				break;
			case CONTAINS:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).toLowerCase().contains(ceValue));
				break;
			case STARTS_WITH:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).toLowerCase().startsWith(ceValue));
				break;
			case ENDS_WITH:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).toLowerCase().endsWith(ceValue));
				break;
			case EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).toLowerCase().equals(dd.getValueByName(itemName)));
				break;
			case NOT_EQUAL_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> !dd.getValueByName(dataItem.getName()).toLowerCase().equals(dd.getValueByName(itemName)));
				break;
			case CONTAINS_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).toLowerCase().contains(dd.getValueByName(itemName)));
				break;
			case STARTS_WITH_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).toLowerCase().startsWith(dd.getValueByName(itemName)));
				break;
			case ENDS_WITH_FIELD:
				itemName = dataItem.getValue();
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).toLowerCase().endsWith(dd.getValueByName(itemName)));
				break;
			case IN:
				criterion = criterionPredicate(dd -> dd.isValueInByName(dataItem.getName(), dataItem.getValues()));
				break;
			case EMPTY:
				criterion = criterionPredicate(dd -> !dd.isValueAssigned(dataItem.getName()));
				break;
			case REGEX:
				criterion = criterionPredicate(dd -> dd.getValueByName(dataItem.getName()).toLowerCase().matches(ceValue));
				break;
			case SORT:
				Data.Order sortOrder = Data.Order.valueOf(dataItem.getValue());
				if (sortOrder == Data.Order.ASCENDING)
					criterion = criterionSortAscending(comparing(dd -> dd.getValueByName(dataItem.getName()).toLowerCase()));
				else
					criterion = criterionSortDescending(comparing(dd -> dd.getValueByName(dataItem.getName()).toLowerCase()));
				break;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(criterion);
	}

	protected void prepare(DSCriteria aDSCriteria, int anOffset, int aLimit)
		throws DSException
	{
		String itemName;
		Data.Type dataType;
		Optional<Criterion> optCriterion;
		DataItem ceDataItem, schemaDataItem;
		Logger appLogger = mAppCtx.getLogger(this, "prepare");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		reset();
		mLimit = aLimit;
		mOffset = anOffset;
		if ((aDSCriteria != null) && (aDSCriteria.isSimple()))
		{
			List<Criterion> ddCriterionList = new ArrayList<>();
			for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
			{
				ceDataItem = ce.getItem();
				itemName = ceDataItem.getName();
				schemaDataItem = mSchema.getItemByName(itemName);
				if (schemaDataItem == null)
					dataType = ceDataItem.getType();
				else
					dataType = schemaDataItem.getType();
				switch (dataType)
				{
					case Boolean:
						optCriterion = booleanCriterion(ce);
						optCriterion.ifPresent(ddCriterionList::add);
						break;
					case Integer:
						optCriterion = integerCriterion(ce);
						optCriterion.ifPresent(ddCriterionList::add);
						break;
					case Long:
						optCriterion = longCriterion(ce);
						optCriterion.ifPresent(ddCriterionList::add);
						break;
					case Float:
						optCriterion = floatCriterion(ce);
						optCriterion.ifPresent(ddCriterionList::add);
						break;
					case Double:
						optCriterion = doubleCriterion(ce);
						optCriterion.ifPresent(ddCriterionList::add);
						break;
					case Date:
					case DateTime:
						optCriterion = dateCriterion(ce);
						optCriterion.ifPresent(ddCriterionList::add);
						break;
					default:
						if (aDSCriteria.isCaseSensitive())
							optCriterion = stringSensitiveCriterion(ce);
						else
							optCriterion = stringInsensitiveCriterion(ce);
						optCriterion.ifPresent(ddCriterionList::add);
						break;
				}
			}
			/* We are deferring the applying of offset and limit so that we can determine the
			   total number of potential matches for UI applications with virtual scrolling.
			if (anOffset > 0)
				ddCriterionList.add(criterionWithOffset(anOffset));
			if (aLimit > 0)
				ddCriterionList.add(criterionWithLimit(aLimit));
			*/
			mCriteria = ddCriterionList.stream().reduce(c -> c, (c1, c2) -> (s -> c2.apply(c1.apply(s))));
		}
		else
			throw new DSException("Cannot prepare criteria - no entries found.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	protected void prepare(DSCriteria aDSCriteria, int aLimit)
		throws DSException
	{
		prepare(aDSCriteria, 0, aLimit);
	}

	protected void prepare(DSCriteria aDSCriteria)
		throws DSException
	{
		int queryOffset = DS.offsetFromCriteria(aDSCriteria);
		int queryLimit = DS.limitFromCriteria(aDSCriteria);
		prepare(aDSCriteria, queryOffset, queryLimit);
	}

	protected void reset()
	{
		Logger appLogger = mAppCtx.getLogger(this, "reset");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mCriteria = null;
		mLimit = DS.CRITERIA_QUERY_LIMIT_DEFAULT;
		mOffset = DS.CRITERIA_QUERY_OFFSET_DEFAULT;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	protected DataGrid execute(DataGrid aDataGrid)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "execute");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);
		if (aDataGrid == null)
			throw new DSException("Cannot execute - data grid is null.");
		if (mCriteria == null)
			throw new DSException("Cannot execute - criteria was not prepared.");

		DataGrid dataGrid = new DataGrid(aDataGrid.getColumns());
		List<DataDoc> completeDocList = mCriteria.apply(aDataGrid.stream()).collect(toList());
		int totalRowCount = completeDocList.size();
		List<DataDoc> trimmedDocList = completeDocList.stream().skip(mOffset).limit(mLimit).collect(toList());
		trimmedDocList.forEach(dataGrid::addRow);

// Assign result set summary features

		if (totalRowCount == 0)
			dataGrid.addFeature(DS.FEATURE_NEXT_OFFSET, 0);
		else
			dataGrid.addFeature(DS.FEATURE_NEXT_OFFSET, Math.min(totalRowCount-1, mOffset + mLimit));
		dataGrid.addFeature(DS.FEATURE_CUR_LIMIT, mLimit);
		dataGrid.addFeature(DS.FEATURE_CUR_OFFSET, mOffset);
		dataGrid.addFeature(DS.FEATURE_TOTAL_DOCUMENTS, totalRowCount);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}
}
