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
