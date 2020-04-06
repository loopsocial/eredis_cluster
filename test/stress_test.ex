defmodule LoadTest do
  @key "fooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
  @value "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaar"
  @command ["SET", @key, @value]

  def run(n \\ 1000) do
    counter = :counters.new(3, [:write_concurrency])

    counter
    |> repeat(0, n)
    |> print_result()

    print_until_finish(counter)
  end

  def repeat(counter, n, n), do: counter

  def repeat(counter, i, n) do
    print_pool_status(i, n)
    async_query(counter)
    repeat(counter, i + 1, n)
  end

  def result(counter) do
    [
      ok: :counters.get(counter, 1),
      error: :counters.get(counter, 2),
      total: :counters.get(counter, 3)
    ]
  end

  @random_query_limit 1000
  defp async_query(counter) do
    number_of_queries = :rand.uniform(@random_query_limit)

    Task.async(fn ->
      Enum.each(1..number_of_queries, fn index ->
        {result, _} = query(index)
        count(counter, result)
      end)
    end)

    count(counter, :total, number_of_queries)
  end

  defp query(_index), do: :eredis_cluster.q(@command)

  defp count(counter, :ok), do: :counters.add(counter, 1, 1)
  defp count(counter, :error), do: :counters.add(counter, 2, 1)

  defp count(counter, :total, number_of_queries) do
    :counters.add(counter, 3, number_of_queries)
  end

  defp measure(function) do
    function
    |> :timer.tc()
    |> elem(0)
    |> Kernel./(1_000_000)
  end

  # 10 times print pool status
  defp print_pool_status(i, n) when rem(i, div(n, 10)) == 0 do
    IO.puts("-> Iteration: #{i}/#{n}")
    print_pool_status()
  end

  defp print_pool_status(_i, _n), do: :ok

  def print_pool_status() do
    :eredis_cluster_monitor.get_all_pools()
    |> Enum.each(fn pool ->
      pool
      |> :poolboy.status()
      |> inspect()
      |> IO.puts()
    end)
  end

  defp print_until_finish(counter) do
    [ok: ok, error: error, total: total] = result(counter)

    if ok + error == total do
      :ok
    else
      if rem(ok + error, div(total, 10)) == 0 do
        print_result(counter)
        print_pool_status()
      end

      print_until_finish(counter)
    end
  end

  defp print_result(counter), do: counter |> result() |> inspect() |> IO.puts()
end
