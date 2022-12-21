exp_compare_barplot <- function(df_benchmark, var = median) {
  df_benchmark |> 
    select(expression, {{ var }}) |> 
    mutate(
      expression = forcats::fct_reorder(as.character(expression),
                                        as.numeric({{ var }})),
      {{ var }} := as.numeric({{ var }})) |> 
    ggplot() +
    aes(expression, {{ var }}) +
    geom_bar(stat = "identity",
             fill = "#D14D7A") +
    coord_flip() +
    xlab(NULL) +
    gghighlight::gghighlight({{ var }} == df_benchmark |> 
                               pull({{ var }}) |> 
                               min(),
                             unhighlighted_params = list(fill = "#524A52"))
}

kansuji2arabic <- function(x) {
  purrr::map_chr(x, function(x) {
    x <- prettyNum(x, scientific = FALSE) %>% 
      arabic2kansuji::arabic2kansuji_all()
    x.kansuji <- stringr::str_split(x, pattern = "[^〇一二三四五六七八九十百千]")[[1]]
    x.kansuji[x.kansuji == ""] <- NA
    x.kansuji <- stats::na.omit(x.kansuji)
    for (i in 1:length(x.kansuji)) {
      if (is.na(x.kansuji[i])) 
        break
      x <- stringr::str_replace(x, pattern = x.kansuji[i], 
                                replacement = prettyNum(zipangu::kansuji2arabic_num(x.kansuji[i])))
    }
    x
  })
  
}
