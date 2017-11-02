### POPULATION DATA FUNCTIONS ###

PLACE_POPULATION2010_RDS_URL <- 'http://cims.nyu.edu/~tlaetsch/PLACE_POPULATION2010_DF.rds'
PLACE_POPULATION2010_RDS_FP <- NULL

# function to retrieve the population data frame
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

# the variable used to house the population data frame
PLACE_POPULATION2010_DF <- PLACE_POPULATION2010()

# function to pull out the subset of data pertaining to a particular place
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


### HELPER FUNCTIONS ###
# File paths and naming convention helper functions
cityAdj <- function( city ) gsub( pattern = '[ ]+', replacement = '', city )

prevCntsFP_creator <- function( input_dir, city, state ) paste( input_dir, 
                                                                sprintf('%s_%s_MHC.csv', cityAdj(city), state), 
                                                                sep = '/')

prevCntsBackupFP_creator <- function( input_dir, city, state ) paste( input_dir, 
                                                                      sprintf('%s_%s_MHC_prev.csv', cityAdj(city), state), 
                                                                      sep = '/')

prevCntsDatedBackupFP_creator <- function( input_dir, city, state ) paste( input_dir, 
                                                                           sprintf('%s_%s_MHC_%s.csv', cityAdj(city), state, Sys.Date()), sep = '/')

logFP_creator <- function( log_dir, city, state ) paste( log_dir, 
                                                         sprintf('%s_%s.log', cityAdj(city), state), 
                                                         sep = '/') 

# convert months (test or numeric) to numeric value (1 <- Jan, 2 <- Feb, ...)
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

# main function to format and merge population data with homicide counts
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

# function to save the formatted population data
save_pop_monthly_cnts <- function( df, output_dir, verbose = F ){
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
     if( verbose ) cat( sprintf("Output filepath to be saved: %s\n", output_fp) )
     write.csv( x = df, file = output_fp, row.names = F )
     if( verbose ) cat( 'File saved.\n')
     
     if( verbose ){
          cat( 'Compare the two data sources:\n')
          cat( "---------ORIGINAL---------\n" )
          dplyr::glimpse( df )
          cat( "---------SAVED---------\n" )
          dplyr::glimpse( read.csv( output_fp, stringsAsFactors = F ) )
          cat( "----------------------------\n")
          cat("Done.\n")
     }
     
     output_fp
}

### HOMICIDE COUNT UPDATES AND SAVING

# funciton to fill in missing months with 0 counts
fillMissingCounts <- function( yr_mo_cnt.df ){
     n_inc <- nrow( yr_mo_cnt.df )
     if( n_inc <= 1 ) return( tuc18_clean )
     df <- yr_mo_cnt.df[ order(yr_mo_cnt.df$year, yr_mo_cnt.df$month), c('year','month','homicide_count')]
     first_year <- df$year[1]
     first_month <- df$month[1]
     last_year <- df$year[n_inc]
     last_month <- df$month[n_inc]
     years <- c()
     months <- c()
     for( year in first_year:last_year ){
          if( (year == first_year) & (first_year < last_year) ){
               months <- first_month:12
               years <- rep( year, length( months ) )
          } else if( (year == last_year) & (first_year < last_year) ){
               new_months <- 1:last_month
               years <- c( years, rep(year, length(new_months)) )
               months <- c( months, new_months )
          } else if( (year == last_year) & (first_year == last_year) ){
               new_months <- first_month:last_month
               years <- c( years, rep(year, length(new_months)) )
               months <- c( months, new_months )
          } else {
               years <- c( years, rep(year, times = 12) )
               months <- c( months, 1:12 )
          }
     }
     zed <- data.frame( year = years, month = months, homicide_count = 0 )
     # return the filled in data set
     as.data.frame( 
          dplyr::summarise( 
               dplyr::group_by( rbind( df, zed ), 
                                year, 
                                month ), 
               homicide_count = sum(homicide_count) )
     )
}

# funciton to merge new counts data with previous counts data
mergeHistorical <- function( hist_df, new_df ){
     
     count_updator <- function( count, hist_ ){
          count[ hist_ == max(hist_) ]
     }
     
     hist_df$hist_ <- 0
     new_df$hist_ <- 1
     merged_df <- merge( hist_df, new_df, all = T )
     # return
     as.data.frame( 
          dplyr::summarise( 
               dplyr::group_by( merged_df, year,month,city,state), 
               homicide_count = count_updator(homicide_count,hist_) 
          ) 
     )
     
}


# writes out the log file created during updating counts
logUpdater <- function( log_dir, 
                              city, 
                              state, 
                              success_retrieving_old_counts = F,
                              success_retrieving_new_counts = F,
                              oldest_date = NA,
                              newest_date = NA,
                              n_row = NA,
                              notes = '' ){
     return_val <- FALSE
     row_update <- data.frame( city = city, 
                               state = state,
                               date = as.character(Sys.time()),
                               success_retrieving_old_counts = success_retrieving_old_counts,
                               success_retrieving_new_counts = success_retrieving_new_counts,
                               oldest_date = oldest_date,
                               newest_date = newest_date,
                               n_row = n_row,
                               notes = notes )
     
     fp <- logFP_creator( log_dir, city, state )
     if( file.exists(fp) ){
          log_df <- read.csv( fp, stringsAsFactors = F )
          n_row <- nrow(log_df)
     } else {
          log_df <- NULL
          n_row <- 0
     }
     log_df <- rbind( log_df, row_update )
     if( nrow( log_df ) == n_row + 1 ){
          return_val <- TRUE
          write.csv( x = log_df, file = fp, row.names = F )
     }
     return_val
}

# the main function, putting together and logging all pieces
MonthlyHomCntsUpdater <- function( input_dir, 
                                   output_dir,
                                   log_dir,
                                   city,
                                   state, 
                                   new_counts_df_creator ){
     # initialize return vals
     success_retrieving_old_counts <- F
     success_retrieving_new_counts <- F
     oldest_date <- NA
     newest_date <- NA
     n_row <- NA
     notes <- NA
     
     # deal with updating notes throughout the counts update
     update_notes <- function( msg ){
          if( is.na(notes) ) return( msg )
          paste( notes, msg, sep = ' | ')
     }
     
     # create the necessary file paths for file io
     prevFP <- prevCntsFP_creator( input_dir, city, state ) #'Tucson', 'AZ' )
     prevBackupFP <- prevCntsBackupFP_creator( input_dir, city, state ) # 'Tucson', 'AZ' )
     prevDatedFP <- prevCntsDatedBackupFP_creator( input_dir, city, state ) #'Tucson', 'AZ' )
     
     # file io function
     prev_file_io <- function( from, to ){
          tryCatch(
               file.copy( from = from, to = to, overwrite = T ),
               error = function( e ) sprintf( "Previous file IO Error: %s", e$message )
          )
     }
     
     # deal with reading in old counts
     prevCnts_df <- NULL
     if( file.exists(prevFP) ){
          prevCnts_df <- read.csv( prevFP, stringsAsFactors = F )
          if( nrow(prevCnts_df) > 0 ){ 
               success_retrieving_old_counts <- T
               if( file.exists(prevBackupFP) ){
                    msg <- prev_file_io( from = prevBackupFP, to = prevDatedFP )
                    if( class(msg) == 'character' ) notes <- update_notes( msg )
               }
               msg <- prev_file_io( from = prevFP, to = prevBackupFP )
               if( class(msg) == 'character' ) notes <- update_notes( msg )
          }
     }
     if( !success_retrieving_old_counts ){
          if( file.exists(prevBackupFP) ){
               prevCnts_df <- read.csv( prevBackupFP, stringsAsFactors = F )
               if( nrow(prevCnts_df) > 0 ){
                    notes <- update_notes('Could not find previous counts file, updating from backup counts file')
                    msg <- prev_file_io( from = prevBackupFP, to = prevDatedFP )
                    if( class(msg) == 'character' ) notes <- update_notes( msg )
               } else {
                    prevCnts_df <- NULL
               }
          }
     }
     
     # get updated new counts 
     newCnts_df <- tryCatch( new_counts_df_creator(), #aggMonthlyHoms18( tuc18_homdf_creator() ),
                             error = function( e ) e$message )
     if( class(newCnts_df) == 'character' ){
          notes <- update_notes( sprintf("Error while getting new counts: %s", newCnts_df) )
          newCnts_df <- NULL
     } else if( class(newCnts_df) == 'data.frame' ){
          if( nrow(newCnts_df) > 0 ){ 
               success_retrieving_new_counts <- T
          } else {
               notes <- update_notes( 'No errors thrown, but no new counts received' )
               newCnts_df <- NULL
          }
     }
     
     # if successfully updated new counts, deal with io
     if( success_retrieving_new_counts ){
          if( !is.null(prevCnts_df) ) newCnts_df <- mergeHistorical( prevCnts_df, newCnts_df )
          newCnts_df <- newCnts_df[ order(newCnts_df$year, newCnts_df$month), ]
          msg <- tryCatch( write.csv( x = newCnts_df, file = prevFP, row.names = F ),
                           error = function( e ) sprintf( 'Error writing out updated counts: %s', e$message )  )
          if( class(msg) == 'character' ){
               notes <- update_notes( msg )
          } else {
               df <- tryCatch( read.csv( prevFP, stringsAsFactors = F ),
                               error = function(e) sprintf('Error reading back in updated counts: %s', e$message) )
               if( class(df) == 'character' ){
                    notes <- update_notes( df )
               } else if( class(df) == 'data.frame' ) {
                    n_row <- nrow(df)
                    oldest_date <- paste(df$year[1],newCnts_df$month[1],sep='-')
                    newest_date <- paste(df$year[n_row],newCnts_df$month[n_row],sep='-')
               }
          }
     }
     # merge with population data
     if( !is.na(n_row) ){
          cntsPop_df <-  tryCatch( format_monthly_hom_cnts( newCnts_df ),
                                   error = function( e ) sprintf( "Error merging with population data: %s", e$message ) )
          if( class(cntsPop_df) == 'character' ){
               notes <- update_notes( cntsPop_df )
               cntsPop_df <- NULL
          } else {
               msg <- tryCatch( save_pop_monthly_cnts( cntsPop_df, output_dir = output_dir ),
                                error = function( e ) sprintf("Error saving population merged df: %s", e$message) )
               notes <- update_notes( msg )
          }
     }
     # update the log
     logUpdater( log_dir = log_dir,
                       city = city, 
                       state = state,
                       success_retrieving_old_counts = success_retrieving_old_counts,
                       success_retrieving_new_counts = success_retrieving_new_counts,
                       oldest_date = oldest_date,
                       newest_date = newest_date,
                       n_row = n_row,
                       notes = notes )
}