PLACE_POPULATION2010_RDS_URL <- 'http://cims.nyu.edu/~tlaetsch/PLACE_POPULATION2010_DF.rds'
PLACE_POPULATION2010_RDS_FP <- NULL

PLACE_POPULATION2010 <- function( var_name = 'PLACE_POPULATION2010_DF',
                                  rds_url = PLACE_POPULATION2010_RDS_URL,
                                  rds_fp = PLACE_POPULATION2010_RDS_FP,
                                  #csv_fp = PLACE_POPULATION2010_CSV_FP,
                                  use_existing = T,
                                  rewrite_rds = F ){
     
     pop_df <- NULL
     if( use_existing ){
          for( e in search() ){
               if( exists(var_name, envir = as.environment(e)) ){
                    pop_df <- get( var_name, envir = as.environment(e) )
                    break
               }
          }
     }
     if( is.null(pop_df) ) {
          if( use_existing & !is.null(rds_fp) ){
               if( file.exists(rds_fp) ){
                    pop_df <- readRDS( file = rds_fp )
                    return( pop_df )
               }
          } else {
               if( !is.null( rds_fp ) & rewrite_rds ){
                    download.file( url = PLACE_POPULATION2010_RDS_URL, destfile = rds_fp )
                    pop_df <- readRDS( rds_fp )
               } else {
                    tmp <- tempfile()
                    download.file( url = PLACE_POPULATION2010_RDS_URL, destfile = tmp )
                    pop_df <- readRDS( tmp )
                    unlink( tmp )
               }
          }
     }
     pop_df
}

PLACE_POPULATION2010_DF <- PLACE_POPULATION2010()

month_to_int_converter <- function( x ){
     int_mnths <- 1:12
     suppressWarnings( int_x <- as.integer(x) ) 
     if( int_x %in% int_mnths ) return( int_x )
     is_substr <- function( sub_str, sup_str ){
          grepl(tolower(sub_str), tolower(sup_str))
     }
     if( is_substr('jan', x) ){
          return( 1L )
     } else if( is_substr('feb',x) ){
          return( 2L )
     } else if( is_substr('mar',x) ){
          return( 3L )
     } else if( is_substr('apr',x) ){
          return( 4L )
     } else if( is_substr('may',x) ){
          return( 5L )
     } else if( is_substr('jun',x) ){
          return( 6L )
     } else if( is_substr('jul',x) ){
          return( 7L )
     } else if( is_substr('aug',x) ){
          return( 8L )
     } else if( is_substr('sep',x) ){
          return( 9L )
     } else if( is_substr('oct',x) ){
          return( 10L )
     } else if( is_substr('nov',x) ){
          return( 11L )
     } else if( is_substr('dec',x) ){
          return( 12L )
     } 
     return( NA )
}

place_pop_extractor <- function( state_abr, place_name ){
     
     pop_df <- PLACE_POPULATION2010()
     
     sub_df <- pop_df[ pop_df$state_abr == toupper(state_abr), ]
     if( nrow(sub_df) == 0 ){
          stop( sprintf( "No matching state abr for %s", state_abr) )
     } 
     sub_df <- sub_df[ sapply(sub_df$place_name, FUN = function(x){ grepl( place_name, x, ignore.case = T ) } ) , ]
     if( nrow(sub_df) == 0 ){
          stop( sprintf( "No matching place names for %s", place_name) )
     } 
     sub_df <- sub_df[order( sapply(sub_df$place_name, nchar  ) ), ]
     
     cat(sprintf("Using population data for %s (fips:%s), %s (fips:%s).\n", 
                 sub_df$place_name[1], 
                 sub_df$place_fips[1], 
                 sub_df$state_name[1], 
                 sub_df$state_fips[1] ) )
     
     return( sub_df[ sub_df$place_name == sub_df$place_name[1], ] )
}

format_monthly_hom_cnts <- function( 
                                        counts_fp_or_df,
                                        state_col = 'state',
                                        place_col = 'city',
                                        year_col = 'year',
                                        month_col = 'month',
                                        hmcnt_col = 'count',
                                        hom_type_col = NULL,
                                        use_hom_types = NULL,
                                        state_abr = NULL,
                                        place_name = NULL,
                                        col_order = c('year',
                                                      'month',
                                                      'state_fips',
                                                      'state_abr',
                                                      'state_name',
                                                      'place_fips',
                                                      'place_name',
                                                      'within_county',
                                                      'population_est',
                                                      'homicide_count'),
                                        ...
                                   ){
     require( dplyr )
     
     
     # read in and clean the monthly counts file passed
     if( class(counts_fp_or_df) == 'data.frame' ){
          cnts_df <- counts_fp_or_df
     } else {
          cnts_df <- read.csv( counts_csv_fp, stringsAsFactors = F, ... )
     }
     # normalize the names of cnts_df for ease of finding the desired columns
     cnts_names <- sapply( names(cnts_df), FUN = function( nm ){ tolower( gsub(" +", "", nm) ) } )
     names( cnts_df ) <- cnts_names
     # normalize the column name for the state abr
     state_col <- tolower( gsub(" +", "", state_col) )
     state_col <- cnts_names[sapply( cnts_names, FUN = function(x){ grepl(state_col, x) } )]
     # normalize the column name for the place
     place_col <- tolower( gsub(" +", "", place_col) )
     place_col <- cnts_names[sapply( cnts_names, FUN = function(x){ grepl(place_col, x) } )]
     # normalize the column name for the year
     year_col <- tolower( gsub(" +", "", year_col) )
     year_col <- cnts_names[sapply( cnts_names, FUN = function(x){ grepl(year_col, x) } )]
     # normalize the column name for the month
     month_col <- tolower( gsub(" +", "", month_col) )
     month_col <- cnts_names[sapply( cnts_names, FUN = function(x){ grepl(month_col, x) } )]
     # normalize the column name for the homicide counts
     hmcnt_col <- tolower( gsub(" +", "", hmcnt_col) )
     hmcnt_col <- cnts_names[sapply( cnts_names, FUN = function(x){ grepl(hmcnt_col, x) } )]
     
     if( !is.null(hom_type_col) & !is.null(use_hom_types) ){
          # if provided, normalize the column name for the homicide types
          hom_type_col <- tolower( gsub(" +", "", hom_type_col) )
          hom_type_col <- cnts_names[sapply( cnts_names, FUN = function(x){ grepl(hom_type_col, x) } )]
          # keep only the homicide types we want to contribute
          cnts_df <- cnts_df[ sapply(cnts_df[hom_type_col], FUN = function(x){ x %in% use_hom_types}), ]
     }
     
     # check and make sure we have some data remaining
     if( nrow(cnts_df) == 0 ){
          stop( sprintf("No homicide counts data read in from file: %s", counts_csv_fp) )
     }
     
     # decide if we need to pull out the state abr and place name from the data
     if( is.null(state_abr) ) state_abr <- cnts_df[[state_col]][1]
     if( is.null(place_name) ) place_name <- cnts_df[[place_col]][1]
     
     # find the population data according to the state and place above
     pop_df <- place_pop_extractor(state_abr = state_abr, place_name = place_name)
     
     # reduce the cnts_df to the only columns we need to merge with pop_df
     cnts_df <- cnts_df[c(year_col, month_col, hmcnt_col)]
     # standardize the col names for year, month, and homicide counts
     names(cnts_df) <- c('year','month','homicide_count')
     # remove any NA homicide counts
     cnts_df <- cnts_df[ !is.na(cnts_df$homicide_count), ]
     # make sure the months are integers
     cnts_df$month <- sapply( as.character(cnts_df$month), FUN = month_to_int_converter )
     # make sure the years are integers
     cnts_df$year <- as.integer( gsub("[^0-9]","",cnts_df$year) )
     # group_by year and month and sum up counts (in case they were separated by homicide type)
     cnts_df <- cnts_df %>% group_by( year, month ) %>% dplyr::summarise( homicide_count = sum(homicide_count) )
     # join this with population data frame
     ret_df <- merge( cnts_df, pop_df )
     # finalize the orgainzation of the data
     ret_df <- ret_df[col_order][order(ret_df$year, ret_df$month), ]
     # remove index
     rownames(ret_df) <- NULL
     return( ret_df )
}

save_monthly_df <- function( df, output_dir = './' ){
     # Create the place name for the filename
     pl_nm <- df$place_name[1]
     spcs <- gregexpr( pattern = ' ', text = pl_nm )[[1]]
     pl_nm <- gsub(' +', '', substr( pl_nm, start = 1, stop = spcs[length(spcs)] ))
     # get the state abr for the filename
     st_abr <- df$state_abr[1]
     # the suffix of the filename
     suffix <- 'Homicide_Counts.csv'
     # combine these into the finished filename
     output_filename <- paste( c(pl_nm, st_abr, suffix), collapse = "_" )
     # add the output_dir info
     output_fp <- paste( c(output_dir, output_filename), collapse = "/" )
     cat( sprintf("Output filepath to be saved: %s\n", output_fp) )
     write.csv( x = df, file = output_fp, row.names = F )
     cat( 'File saved.\n')
     
     cat( 'Compare the two data sources:\n')
     cat( "---------ORIGINAL---------\n" )
     dplyr::glimpse( df )
     cat( "---------SAVED---------\n" )
     dplyr::glimpse( read.csv( output_fp, stringsAsFactors = F ) )
     cat( "----------------------------\n")
     cat("Done.\n")
}