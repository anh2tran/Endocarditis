##Adapt from codes of Stephane Le Vu
##All the codes belong to Stephane Le Vu, I just add one condition that the non-exposed subjects need to be alive at the date of matching

## Sampling 1:k without replacement for exact matching of
## controls to treated

##---- make data ----
#' Simulate exposed / unexposed with prevalence and matching covariates
#'
#' @param npop Population size.
#' @param pe Prevalence of exposure.
#' @param mvars Vector names of matching covariates.
#' @param kmvars Number of categories per each covariates.
#' @param idvar Name of identifier.
#' @param expovar Name of exposed/treated variable.
#' @param expodat Name of date of exposure variable (in days).
#' @param timedep Option to simulate dates of exposure
#' @param endobs Maximum date of exposure.
#' @param seed Random seed.
#' @returns A dataframe.
#' @importFrom stats rbinom runif setNames
#' @export
make_data <- function(npop = 10,
                      pe = .2,
                      mvars = c("sex", "age", "reg"),
                      kmvars = c(2, 4, 4),
                      idvar = "id",
                      expovar = "expo",
                      expodat = "doe",
                      timedep = FALSE,
                      endobs = 365.25,
                      seed = 12
){
  ## tests
  if ( length(mvars) != length(kmvars) ){
    stop("mvars and kmvars must have same length")
  }
  ## id
  id <- seq(npop)
  ## exposure
  set.seed(seed)
  expo <- rbinom(npop, 1, pe)
  ## date of exposure
  if (timedep){
    doe <- as.integer(ceiling(runif(npop) * endobs))
    doe <- ifelse(expo == 1, doe, NA)   }
  ## covariates
  l <- lapply(setNames(1:length(mvars), mvars),
              function(i){
                if (kmvars[i] == 1) return(1) else
                  as.integer(round( runif(npop, 1, kmvars[i]) ))
              })
  d <- data.frame(id, expo, do.call(cbind, l))
  if (timedep) d[, expodat] <- doe
  names(d)[1:2] <- c(idvar, expovar)
  if (sum(d[,expovar]) == 0) warning("no exposed")
  d
}

#---- make_keys ----
#' Split the data by unique combination of matching variables
#'
#' @param d Input dataframe.
#' @param mvars Vector of names for matching variables.
#' @param expovar Name of treated variable.
#' @returns Named list of dataframes.
#' @export
make_keys <- function(d,
                      mvars,
                      expovar){
  ## all keys
  keys <- interaction(d[, mvars], sep = ":")
  ## keys for treated and controls
  keys_by_expo <- split(keys, d[, expovar], drop = TRUE)
  ## indices with potential match
  inter <- intersect(keys_by_expo[[1]], keys_by_expo[[2]])
  # if (length(inter) == 0) stop("No matching possible")
  ki <- keys %in% inter
  ## rm rows with no potential match
  d1 <- d[ki, !(names(d) %in% mvars)]
  rownames(d1) <- NULL
  ## list of strata
  l_ds_by_key <- split(d1, keys[ki], drop = TRUE)
  l_ds_by_key
}

#---- sample_k_ctl ----
#' Sample controls without replacement
#'
#' Returns `NA` if `k` exceeds the number of available controls.
#' @param d Dataframe for one level of matching variables.
#' @param ttval Value of `expovar` for treated.
#' @param k Number of controls per treated.
#' @param maxim Maximize minimum number of controls per case (default = TRUE).
#' @param sorted Sort controls with `NA` last (default = FALSE).
#' @inheritParams make_data
#' @returns A matrix of identifiers for treated (column 1) and
#' matched controls (`k` columns).
sample_k_ctl <- function(d, idvar, expovar, ttval, k, maxim = TRUE,
                         seed = 123, sorted = FALSE){
  ## vectors of id for treated and controls
  it <- d[d[, expovar] == ttval, idvar]
  ic <- d[d[, expovar] != ttval, idvar]
  nt <- length(it)
  nc <- length(ic)
  if (nc == 0) {
    # warning("zero control")
    return(cbind(case = it, ctl = matrix(NA, nrow = nt, ncol = k)))
  }
  if (k > 1 & maxim){
    kmin <- min(floor(nc/nt), k) ## watch out if kmin = 0 !!!!!!!!!!!!!!!!
    set.seed(seed)
    # safer than sample(ic, ...) in case length(x) is 1
    ic1 <- ic[sample(1:length(ic), kmin * nt, replace = FALSE)]
    mat_ic1 <- matrix(ic1, nrow=nt, ncol = kmin, byrow = TRUE)
    ## pad additional controls with NA
    if (kmin < k){
      rest_of_ic <- setdiff(ic, ic1)
      rest_of_ic_and_na <- c(rest_of_ic, rep(NA, nt - length(rest_of_ic) ))
      ic2 <- rest_of_ic_and_na[ sample(1:length(rest_of_ic_and_na),
                                       nt, replace = FALSE) ]
      ic2 <-matrix(ic2, ncol =1)
      padding <- matrix(NA, nrow=nt, ncol = (k-kmin-1))

      stopifnot(nrow(mat_ic1) == nt, nrow(ic2) == nt, nrow(padding) == nt)

      mc <- do.call(cbind, list(mat_ic1, ic2, padding))
    }else {
      mc <- mat_ic1
    }
    # return(cbind(case = it, ctl = mc))
  } else {
    ## pad controls id with NAs to match treated length
    if ( nc < k * nt ) ic <- c(ic, rep(NA, k * nt - nc))
    ## sample controls
    set.seed(seed)
    mc <- ic[ sample(1:length(ic), k * nt, replace = FALSE) ]
    mc <- matrix(mc, ncol = k)
    if (sorted) mc <- t(apply(mc, 1, function(r) sort(r, na.last = TRUE)))
  }
  dimnames(mc) <- list(NULL, paste0("ctl", 1:k))
  ## output
  cbind(ttt = it, ctl = mc)
}

##---- td_sample ----
#' Sequential matching of controls at time of exposure
#'
#' Every individuals start as unexposed at time 0.
#' Matched unexposed can turn to exposed later, censoring the pair.
#'
#' @param x Dataframe for one level of matching variables.
#' @param ... Parameters passed to [sample_k_ctl()]
#' @param death Name of date of death in DCIR or PMSI
#' @param verbose Print matching statistics.
#' @inheritParams make_data
#' @returns A matrix of identifiers for treated (column 1) and
#' matched controls (`k` columns), and dates of exposure/matching.
td_sample <- function(x, expodat, death,expovar, idvar, ..., verbose = FALSE){


  safe_as_date <- function(vec, format = "%d/%m/%Y"){
    if(inherits(vec,"Date")) return(vec)
    if (is.numeric(vec)) return(as.Date(vec, origin = "1970-01-01"))
    supressingWarnings(as.Date(vec, format = format))
  }

  #used in td_sample()
  x[,expodat] <- safe_as_date(x[,expodat])
  x[,death] <- safe_as_date(x[,death])

  s <- sort(unique(x[, expodat]))
  xi <- x
  ce <- vector("integer")
  l <- vector("list", length = length(s)) # list() #
  for (i in seq_along(s)){
    ## censoring and retain newly exposed
    if ( suppressWarnings( length(ce) != 0 & s[i] == min(ce) ) ){
      xi <- rbind(xi, x[!is.na(x[, expodat]) & x[, expodat] == min(ce),])
      xi <- xi[!duplicated(xi[, idvar]),]
      ce <- ce[ce > s[i]] # discard current censoring time
    }
    ## currently exposed
    xi[, expovar] <- ifelse(is.na(xi[, expodat]) | xi[, expodat] > s[i], 0, 1) ## watchout ttval

    ## select controls alive at the index date
    xi <- xi[is.na(xi[,death])|xi[,death] > s[i],]


    if (verbose) print(xi)
    if ( all(xi[, expovar] == 0) ) break
    ## sample
    # browser()
    r <- sample_k_ctl(d = xi, idvar = idvar, expovar = expovar, ...)
    ## watchout debug only
    # r <- sample_k_ctl(xi, idvar = idvar, expovar = expovar, ttval = ttval,
    # k = k, seed = seed)
    l[[i]] <- r # l <- c(l, list(r))
    if (verbose) print(r)
    ## retain controls that will be censored
    ctli <- unlist(r[, -1])
    dc <- xi[xi[, idvar] %in% ctli, expodat]
    ce <- c(ce, dc[!is.na(dc)])
    if (verbose) print(ce)
    # browser()
    ## rm matched pairs
    xi <- xi[!(xi[, idvar] %in% unlist(r)),]
  }
  m <- do.call(rbind, l)
  data.frame(m, dom = x[match(m[,1], x[, idvar]), expodat])
}

##---- find_match ----
#' Find *k* exact matches per treated
#'
#' @param d Input dataframe.
#' @param k Number of controls per treated.
#' @param cores Number of cores for parallel apply (1 for windows).
#' @param timedep Option for sequential matching
#' @param verbose Print stuff.
#' @inheritParams sample_k_ctl
#' @inheritParams make_keys
#' @inheritParams make_data
#' @returns A matrix with indices of treated in first columns
#' and *k* columns of indices of matched controls. If `timedep = TRUE`,
#' a column of date of matching is added
#' @export
find_match <- function(d,
                       mvars,
                       expovar,
                       expodat,
                       death,
                       idvar = NULL,
                       ttval = 1,
                       k = 1,
                       maxim = TRUE,
                       seed = 123,
                       cores = 12,
                       timedep = FALSE,
                       verbose = FALSE){
  if ( is.null(idvar) ){
    if (verbose) print("creating indices")
    idvar <- "id"
    if ( !("id" %in% names(d)) ) d[, idvar] <- 1:nrow(d)
  } else {
    if ( any(duplicated(d[, idvar])) ) stop("idvar must have unique values")
  }
  if ( timedep & !identical(d[,expovar] == ttval, !is.na(d[,expodat])) ){
    stop("dates of exposure must be present when exposed and NA otherwise")
  }
  # require(parallel)
  if(.Platform$OS.type == "windows") {
    if (verbose) warning("set nb cores to 1 on Windows")
    cores <- 1
  }
  d <- as.data.frame(d)
  if (sum(d[, expovar]) == 0) stop("Error: No exposed")
  # if (verbose) print("make list")
  lk <- make_keys(d = d, mvars = mvars, expovar = expovar)
  if (length(lk) == 0) return(cat('There is no match'))
  # if (verbose) print("sample")
  lm <- parallel::mclapply(1:length(lk), function(i){
    if (timedep){
      td_sample(x = lk[[i]],
                expodat = expodat,
                idvar = idvar,
                expovar = expovar,
                death = death,
                ttval = ttval,
                k = k,
                maxim = maxim,
                seed = seed)
    } else {
      sample_k_ctl(d = lk[[i]],
                   idvar = idvar,
                   expovar = expovar,
                   ttval = ttval,
                   k = k,
                   maxim = maxim,
                   seed = seed)
    }
  }, mc.cores = cores)
  m <- do.call(rbind, lm)
  if (verbose){
    nb_ttt <- sum(d[, expovar] == ttval)
    nb_ctl_per_matched_ttt <- apply(m[, grep("ctl", colnames(m)), drop = FALSE], 1,
                                    function(r) sum(!is.na(r)))
    a <- sum(nb_ctl_per_matched_ttt != 0)
    cat("----\n")
    cat(paste(nb_ttt, "treated;", a, "matched;", round(a/nb_ttt*100, 2), "%\n" ))
    print(table(nb_ctl_per_matched_ttt))
    cat("----\n")
  } ## todo : more matching reports
  m
}

##---- make_strata ----
#' Derive stratum vector of matched treated and controls
#'
#' @param d Input dataframe.
#' @param r The matrix output of [find_match()].
#' @inheritParams make_data
#' @returns If exposure status was fixed (`timedep = FALSE`),
#' an integer vector of length `nrow(d)`.
#' Value zero corresponds to no matching.
#' If exposure was time-dependent, a `nrow(d)` by 2 matrix
#' where second strata column might differ for matched unexposed
#' that turned exposed and subsequently matched.
#' @export
make_strata <- function(d, r, idvar = NULL){
  if ( is.null(idvar) ){
    idvar <- "id"
    if ( !("id" %in% names(d)) ) d[, idvar] <- 1:nrow(d)
  }
  r <- r[, grep("ttt|ctl", colnames(r))]
  strata <- vector("list", nrow(d))
  for (i in 1:nrow(r)){
    x <- r[i, ]
    u <- match(x[!is.na(x)], d[, idvar])
    strata[ u ] <- lapply(strata[ u ], function(a) c(a, i))
  }
  ## all subjects
  strata <- lapply(strata, function(x) if (is.null(x)) 0L else x)
  do.call(rbind, strata)
}


##---- process_seq_matching ----
## assumes id, doe, etc. are variables !
globalVariables(c("doe", "id", "end_uu", "doum", "end_um", "end_e", "ttt",
                  "ctl", "dom", "doc", "keys"))
#' Prepare variables for plotting
#' @param d Input dataframe.
#' @param m The matrix output of [find_match()].
#' @param endobs Maximum date of exposure.
#' @inheritParams make_data
#' @export
#' @importFrom stats reshape
process_seq_matching <- function(d, m, idvar, expodat,
                                 mvars, endobs = 365){
  if ( !(idvar %in% names(d)) ) d[, idvar] <- 1:nrow(d)
  dd <- d
  mm <- reshape(m, varying = 2:(ncol(m) - 1), sep = "", direction = "long")
  mm <- mm[!is.na(mm$ctl),]
  dd$id <- factor(dd[, idvar], levels = unique(dd[, idvar]))
  dd$doe <- dd[, expodat]
  ## add info
  # mvars <- names(dd)[!(names(dd) %in% c("id", "expo", "doe"))]
  dd$keys <- interaction(d[, mvars], sep = ":")
  dd$doc <- dd$doum <- dd$end_e <- NA
  doec <- dd[ match(mm[, "ctl"], dd[, idvar]), expodat] ## censoring of unexposed getting exposed
  end_e <- with(mm, tapply(doec, ttt, function(x) if (any(is.na(x))) NA else x) )
  doc <- with(mm, tapply(doec, ttt, function(x) if (all(is.na(x))) NA else min(x, na.rm = TRUE)) )
  dd[match(names(end_e), dd[, idvar]), "end_e"] <- sapply(end_e, function(x){
    if (all(is.na(x))) endobs else min(x, na.rm = TRUE) }) # ifelse(is.na(end_e), endobs, min(end_e, na.rm = TRUE))
  dd[match(names(doc), dd[, idvar]), "doc"] <- doc ## matched controls censor their treated pair at exposure
  dd[match(mm[, "ctl"], dd[, idvar]), "doum"] <- d[ match(mm[, "ttt"], d[, idvar]), expodat] ## when controls start to be matched
  ## unexposed unmatched starts at 0
  dd$end_uu <- with(dd, ifelse(is.na(doum), ifelse(is.na(doe), endobs, doe), doum))
  dd$end_um <- with(dd, ifelse(is.na(doe), endobs, doe))
  list(dd = dd, mm = mm)
}

##---- plot_seq_matching ----
#' Plotting function
#' @param dm The list output of [process_seq_matching()].
#' @param labx Label for x time axis.
#' @param sort_key Option to sort subject indices by their covariates set.
#' @export
#' @import ggplot2
plot_seq_matching <- function(dm, labx = "t", sort_key = FALSE){
  # require(ggplot2)
  dd <- dm[["dd"]]
  mm <- dm[["mm"]]
  if (sort_key){
    dd$id <- with(dd, reorder(id, keys, as.integer))
    mm$ttt <- factor(mm$ttt, levels = levels(dd$id))
    mm$ctl <- factor(mm$ctl, levels = levels(dd$id))
  } ## nice reordering
  ggplot(dd, aes(doe, id)) +
    geom_segment(aes(y = id, yend = id,
                     x = 0, xend = end_uu),
                 colour = 4, linetype = 3)  + ## unexposed & unpaired
    geom_segment(aes(y = id, yend = id,
                     x = doum, xend = end_um),
                 linewidth = 1,
                 colour = 4, linetype = 1, na.rm = TRUE)  + ## unexposed & paired
    geom_segment(aes(y = id, yend = id,
                     x = end_um,
                     xend = end_e),
                 linewidth = 1,
                 colour = 2, na.rm = TRUE)  + ## exposed
    geom_segment(data = mm, aes(y = factor(match(ttt, dd$id), levels = levels(dd$id)), # match(ttt, dd$id),
                                yend = factor(match(ctl, dd$id), levels = levels(dd$id)), # match(ctl, dd$id),
                                x = dom,
                                xend = dom),
                 arrow = arrow(length = unit(0.17, "cm")),
                 linetype = 1) + ## matching (vertical)
    geom_point(size = 2, shape = 21, colour = 2, fill = 2, na.rm = TRUE) + ## exposure
    {if ( !all(is.na(dd$doc)) )
      geom_point(aes(doc, id), size = 2, shape = 23, fill = "white", na.rm = TRUE) ## censoring
    } +
    xlab(labx) +
    theme_classic() +
    theme(axis.line.y = element_blank(),
          axis.ticks.y = element_blank(),
          axis.text.y = element_text(
            colour = if (sort_key) ifelse(as.integer(sort(dd$keys)) %% 2 == 0, 8, 1) else 1,
            face = if (sort_key) ifelse(as.integer(sort(dd$keys)) %% 2 == 0, "plain", "bold") else "plain"
          )
    )

}

##---- import_sas_snds ----
#' Wrapper for \code{\link[haven]{read_sas}}
#'
#' @param PATHSAS Path to `*.sas7bdat` file.
#' @param exclude Vector of variable names to exclude from import.
#' @param n Number of lines to import (for testing).
#' @examples
#' path <- system.file("extdata/d0.sas7bdat",
#' package = "appart", mustWork = TRUE)
#' import_sas_snds(path, n = 2)
#' import_sas_snds(path, exclude = "BEN_IDT_ANO")
#' @export
import_sas_snds <- function(PATHSAS,
                            exclude = NULL, #"BEN_IDT_ANO",
                            # include = NULL,
                            n = Inf){
  # vars_select <- function(){
  #   c( !all_of(exclude), all_of(include) )
  # }
  if ( !( grepl("sas7bdat", basename(PATHSAS)) &
          file.exists(PATHSAS) ) ) stop("Cannot find a *.sas7bdat file")
  d1 <- haven::read_sas(PATHSAS, n_max = n,
                        col_select = !dplyr::all_of(exclude) )# vars_select()
  d1 <- haven::zap_formats(d1)
  d1 <- haven::zap_label(d1)
  as.data.frame(d1)
}

##---- export_sas_snds ----
#' Hack to export R dataframe to sas7bdat file
#'
#' Meant as a substitute for [haven::write_sas()] that does not work.
#' @details
#' Needs an extra step of importing the `*.xpt` file by executing
#' (in SAS) the `xpt2sas_.sas` script generated. Resulting sas7bdat file will be named
#' in the form `<prefix><date>.sas7bdat`.
#' @param x Dataframe like object.
#' @param DN Path to export directory.
#' @param prefix One or two letters forming the name of sas7bdat file.
#' @param verbose Print infos.
#' @examples
#' x <- data.frame(a = 1:2)
#' DN <- tempdir()
#' export_sas_snds(x = x, DN = DN)
#' ( list.files(DN, "xpt|sas", full.names = TRUE) )
#' unlink(tempdir(), recursive = TRUE)
#' @export

## SAS: name is going to be that of  (limited to 8 characters)
export_sas_snds <- function(x, DN, prefix = "m", verbose = TRUE){
  da <- format(Sys.Date(), "%y%m%d")
  FNXPT <- paste0("export", da, ".xpt")
  FNSASDB <- paste0(prefix, da)
  FNSASCO <- paste0(DN, "/", "xpt2sas", da, ".sas")
  haven::write_xpt(as.data.frame(x),
                   paste0(DN, "/", FNXPT),
                   version = 5,
                   name = FNSASDB)
  ##-- generate xpt to sas script
  sascode <- c(paste("/* import xpt to sas", da, "*/"),
               "%let root = %sysget(HOME)/sasdata;",
               paste0('libname DIR ', '\"', '&root',
                      sub('~', '', DN), '\";'),
               paste0("libname XPT xport ", '\"', "&root",
                      sub('~', '', DN), "/", FNXPT, '\";'),
               "proc copy in = XPT out = DIR memtype = data;",
               "run;" )
  writeLines(sascode, FNSASCO)
  if (verbose) {
    cat(paste("Executing sas script",
              FNSASCO,
              "will extract sas database",
              paste0(FNSASDB, ".sas7bdat" ), sep = "\n" ) )
  }
}


