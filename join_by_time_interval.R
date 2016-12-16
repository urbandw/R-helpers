require(dplyr)
require(lubridate)
require(tidyr)

# See http://stackoverflow.com/questions/26619329/dplyr-rename-standard-evaluation-function-not-working-as-expected for explanation of renaming.
# Also here: http://www.carlboettiger.info/2015/02/06/fun-standardizing-non-standard-evaluation.html

join_by_time_interval = function(df1, df2, time_cols, max_delta, delta_units="days", 
                                 other_cols = NULL, how="inner",
                                 closest_only=TRUE, gather_shared_cols=FALSE,
                                 suffix = c("_x", "_y")) {
  
  # Joins dataframes df1 and df2 on their respective columns in time_cols, but not
  # by exact matches. Rather, rows are joined when the respective time values
  # are within max_delta of each other. It is up to the user to make sure the 
  # time columns are compatible.
  
  # Parameters
  # --------------
  # closest_only: bool
  #   should only the closest time matches be retained
  
  # 
  df1 = df1 %>% ungroup # just in case grouped dataframes were passed in
  df2 = df2 %>% ungroup
  if (length(time_cols)==1) time_cols = rep(time_cols, 2)
  if (length(max_delta)==1) max_delta = sort(c(max_delta, -max_delta))
  t1 = df1[[time_cols[1]]]
  t2 = df2[[time_cols[2]]]
  
  # If other columns on which to join aren't specified, use the intersection of all
  # non-time columns. Some different behavior might be the better default here.
  if (is.null(other_cols)) {
    other_cols = setdiff(intersect(names(df1), names(df2)), time_cols)
  }
  # 
  # Tell the user exactly how the join is being performed
  if (any(class(t1) != class(t2))) {
    print(paste("Warning: Subtracting a", class(t2)[1], "from a", class(t1)[1], 
                "might result in unexpected behavior when computing time differences.",
                "Consider setting time classes and/or locales more precisely."))
  }
  print("Joining by near-matches on time columns ...")
  print(paste(time_cols[1], "(left) minus ", time_cols[2], 
              "(right) are between", max_delta[1], "and", max_delta[2], delta_units,
              "inclusive."))
  print(paste("Also joining by exact matches on columns:", 
              paste(other_cols, collapse=", ")))
  if (closest_only) {
    print(paste("Only the closest time matches are retained,",
                "rather than all matches within the specified max_delta",
                "(closest_only==TRUE)"))
  }
  
  # Find matches between unique time values in each input dataframe.
  t1_unique = unique(t1)
  t2_unique = unique(t2)
  fn = function(x, y) as.numeric(difftime(x, y, units=delta_units))
  dt = outer(t1_unique, t2_unique, fn)
  inds = which((dt >= min(max_delta)) & (dt <= max(max_delta)), arr.ind=T)
  time_df = data_frame(time_x = t1_unique[inds[,1]], 
                       time_y = t2_unique[inds[,2]], 
                       time_delta = dt[inds])
  
  if (closest_only) {
    time_df = time_df %>% 
      group_by(time_x) %>% filter(abs(time_delta) == min(abs(time_delta))) %>%
      group_by(time_y) %>% filter(abs(time_delta) == min(abs(time_delta))) %>%
      ungroup
  }
  
  # Shouldn't ever be true; debugging flag
  if (any(duplicated(time_df))) print("Duplicate rows in time_df; shouldn't be possible")
  
  # Merge dataframe defining unique time matches onto respective input dataframes.
  # We do a left_join here to preserve all times without a match, in case how=="left"
  # for the final join.
  
  df11 = df1 %>% 
    # rename_(.dots = setNames(time_cols[1], "time_x")) %>% # alternative method
    rename_(time_x = time_cols[1]) %>% 
    left_join(time_df, by="time_x")
  df21 = df2 %>% 
    rename_(time_y = time_cols[2]) %>% 
    left_join(time_df, by="time_y")
  
  # Final join 
  df3 = do.call(paste(how, "join", sep="_"), 
                list(df11, df21, by=c("time_x", "time_y", other_cols), suffix=suffix))

  # Another debugging check
  if (!all.equal(df3[[paste0("time_delta", suffix[1])]], 
                 df3[[paste0("time_delta", suffix[2])]])) {
    print("Left and right time columns are not identical; shouldn't be possible")
  }
  
  # drop duplicate time_delta column and rename for aesthetics
  df4 = df3 %>%
    rename_(time_delta = paste0("time_delta", suffix[1])) %>%
    select_(paste0("-time_delta", suffix[2]))
  
  first_cols = c('time_x', 'time_y', 'time_delta')
  df4[, c(first_cols, setdiff(names(df4), first_cols))]
  
  df4
}


gather_variable_sets = function(df, var_sets, 
                                key_names = c("source", "variable", "value")) {
  # Parameters:
  # ---------------
  # df: dataframe
  # var_sets: a named list. Each element is a character vector, whose values
  # correspond to column names in the original dataframe that are to be gathered.
  # The vector names denote which data source these variables come from, while the 
  # list names denote whatever this group of variables has in common (e.g. all of 
  # them are temperature columns). These values will populate "source" and "variable"
  # respectively.
  
  # Example:
  # var_sets = list(RH=c(cimis="humidity", field="hum"), 
  #                 Ta=c(cimis="air_temp", field="temp"))
  # would gather the "hum" and "humidity" columns into one "RH" column, and the
  # "temp" and "air_temp" columns into one "Ta" column. A "source" column would designate
  # each observation as either "cimis" or "field".
  
  dfs = list()
  for (i in seq_along(var_sets)) {
    value_name = names(var_sets)[i]
    vars = var_sets[[i]]
    drop_cols = setdiff(unlist(var_sets), unlist(var_sets[[i]]))
    df_i = df %>% ungroup %>% rename_(.dots = vars) %>% 
      gather_(key_names[1], value_name, names(vars))
    if (length(drop_cols) > 0) {
      df_i = df_i %>% select_(.dots = paste0("-", drop_cols))
    }
    dfs[[i]] = df_i %>% gather_(key_names[2], key_names[3], value_name)
  }
  if (any(diff(sapply(dfs, ncol))) != 0) {
    print("Intermediate dataframes having differing numbers of columns. Check that
          var_sets is a named list that correctly identifies the groups of variables
          to be successively gathered.")
    return()
  } else {
    return(bind_rows(dfs))
  }
}

