join_by_time_interval = function(df1, df2, time_cols, max_delta, delta_units="days", 
                                 other_cols = NULL, absolute=TRUE, how="inner",
                                 closest_only=TRUE) {
  
  # Joins dataframes df1 and df2 on their respective columns in time_cols, but not
  # by exact matches. Rather, rows are joined for which the respective time values
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
  t1 = df1[[time_cols[1]]]
  t2 = df2[[time_cols[2]]]
  
  # If other columns on which to join aren't specified, use the intersection of all
  # non-time columns
  if (is.null(other_cols)) {
    other_cols = setdiff(intersect(names(df1), names(df2)), time_cols)
  }
  
  # Tell the user exactly how the join is being performed
  if (class(t1) != class(t2)) {
    print(paste("Warning: Subtracting a", class(t2)[1], "from a", class(t1)[1], 
                "might result in unexpected behavior when computing time differences.",
                "Consider setting time classes and/or locales more precisely."))
  }
  print("Joining on near-matches of time columns ...")
  print(paste0(time_cols[1], " (left dataframe) minus ", time_cols[2], 
               " (right dataframe) less than or equal to ", max_delta, delta_units))
  print(paste("Also joining by exact matches on columns:", 
              paste(other_cols, collapse=", ")))
  if (absolute) {
    print("Signs of time differences ignored (absolute==TRUE)")
  }
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
  if (absolute) {
    inds = which(abs(dt) <= abs(max_delta), arr.ind=T)
  } else {
    inds = which(dt <= max_delta, arr.ind=T)
  }
  time_df = data_frame(time_x = t1_unique[inds[,1]], 
                       time_y = t2_unique[inds[,2]], 
                       time_delta = dt[inds])
  
  if (closest_only) {
    time_df = time_df %>% 
      group_by(time_x) %>% filter(time_delta == min(time_delta)) %>%
      group_by(time_y) %>% filter(time_delta == min(time_delta))
  }
  
  # Shouldn't ever be true; debugging flag
  if (any(duplicated(time_df))) print("Uh oh: duplicate rows in time_df")
  
  # Merge dataframe defining unique time matches onto respective input dataframes.
  # See http://stackoverflow.com/questions/26619329/dplyr-rename-standard-evaluation-function-not-working-as-expected for explanation of renaming.
  df11 = df1 %>% 
    # rename_(.dots = setNames(time_cols[1], "time_x")) %>% # alternative method
    rename_("time_x"=time_cols[1]) %>% 
    left_join(time_df, by="time_x")
  df21 = df2 %>% 
    rename_("time_y"=time_cols[2]) %>% 
    left_join(time_df, by="time_y")
  
  # Final join 
  if (how=="inner") {
    df3 = inner_join(df11, df21, by=c("time_x", "time_y", other_cols))
  } else if (how=="left") {
    df3 = left_join(df11, df21, by=c("time_x", "time_y", other_cols))
  } else if (how=="right") {
    df3 = right_join(df11, df21, by=c("time_x", "time_y", other_cols))
  }
  
  # Another debugging check
  if (!all.equal(df3$time_delta.x, df3$time_delta.y)) {
    print("Uh oh: left and right time columns are not identical")
  }
  
  # drop duplicate time_delta column and rename for aesthetics
  df4 = df3 %>%
    rename(time_delta = time_delta.x) %>%
    select(-time_delta.y)
  
  df4
  
}
