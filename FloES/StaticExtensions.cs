using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nest;

namespace FloES
{
    static class StaticExtensions
    {
        /// <summary>
        /// Construct the sort descriptor using the user inputted sort tuple
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sortDescriptor">Pre-existing SortDescriptor</param>
        /// <param name="sort">User-inputted sort tuple</param>
        public static SortDescriptor<T> ConstructSortDescriptor<T>(
          this SortDescriptor<T> sortDescriptor,
          Tuple<string, string> sort) where T : class
        {
            if (sort != null)
            {
                (string field, string direction) = sort;

                switch (direction)
                {
                    case "asc":
                        sortDescriptor.Ascending(field);
                        break;
                    case "des":
                        sortDescriptor.Descending(field);
                        break;
                }
            }

            return sortDescriptor;
        }

        /// <summary>
        /// Construct the query container descriptor
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="queryContainerDescriptor">Pre-existing QueryContainerDescriptor</param>
        /// <param name="filters">User-inputted filter tuples</param>
        /// <param name="fieldToSearch">User-inputted search field</param>
        /// <param name="valueToSearch">User-inputted search value</param>
        /// <param name="scrollLastXHours">User-inputted scroll last X hours</param>
        /// <param name="scrollLastXDays">User-inputted scroll last X days</param>
        /// <param name="timeStampField"></param>
        public static QueryContainerDescriptor<T> ConstructQueryContainerDescriptor<T>(
          this QueryContainerDescriptor<T> queryContainerDescriptor,
          Tuple<string, string>[] filters,
          string fieldToSearch = null,
          object valueToSearch = null,
          double? scrollLastXHours = null,
          double? scrollLastXDays = null,
          string timeStampField = "timeStamp") where T : class
        {
            if (fieldToSearch != null && valueToSearch != null)
            {
                queryContainerDescriptor.Match(c => c
                  .Field(fieldToSearch)
                  .Query(valueToSearch.ToString()));
            }

            IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> filterFunctions =
              ConstructFilters<T>(filters);

            if (filterFunctions != null)
            {
                queryContainerDescriptor.Bool(b => b
                  .Filter(filterFunctions)
                  .Must(must => must.MatchAll()));
            }

            if (scrollLastXHours != null)
            {
              queryContainerDescriptor.DateRange(s => s
                .Field(timeStampField)
                .GreaterThanOrEquals(
                  DateTime.UtcNow.Subtract(TimeSpan.FromHours(scrollLastXHours.Value))));
            }
            else if (scrollLastXDays != null)
            {
              queryContainerDescriptor.DateRange(s => s
                .Field(timeStampField)
                .GreaterThanOrEquals(
                  DateTime.UtcNow.Subtract(TimeSpan.FromDays(scrollLastXDays.Value))));
            }

            return queryContainerDescriptor;
        }

        /// <summary>
        /// Construct the query functions based on user inputted filter tuples
        /// </summary>
        private static IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> ConstructFilters<T>(
          Tuple<string, string>[] filters) where T : class
        {
            IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> filterFunctions = null;
            if (filters != null)
            {
                filterFunctions = filters.Select(filter =>
                {
                    (string field, string value) = filter;

                    QueryContainer FilterFunction(QueryContainerDescriptor<T> filterFunc) =>
                filterFunc
                  .Term(term => term.Field(field)
                    .Value(value));

                    return (Func<QueryContainerDescriptor<T>, QueryContainer>)FilterFunction;
                });
            }

            return filterFunctions;
        }
    }
}
