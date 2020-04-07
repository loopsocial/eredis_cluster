defmodule StressTest do
  def infinity_loop() do
    counter = :counters.new(3, [:write_concurrency])

    with {:ok, pid} <- Task.start(__MODULE__, :infinity_loop, [counter, 0]) do
      %{counter: counter, pid: pid}
    end
  end

  def infinity_loop(counter, iteration) do
    start_query_task(counter, iteration)
    infinity_loop(counter, iteration + 1)
  end

  def result(%{counter: counter}) do
    result(counter)
  end

  def result(counter) do
    [
      ok: :counters.get(counter, 1),
      error: :counters.get(counter, 2),
      total: :counters.get(counter, 3)
    ]
  end

  def finish(%{counter: counter, pid: pid}) do
    Process.exit(pid, :finish)

    IO.puts("\n>>> Final Result:")
    print_pool_status()
    print_result(counter)
  end

  # Avg of 100 queries per run
  @random_query_limit 200
  defp start_query_task(counter, iteration) do
    number_of_queries = :rand.uniform(@random_query_limit)

    Task.start_link(fn ->
      for i <- 1..number_of_queries do
        {result, _} = query(i)
        count(counter, result)
      end
    end)

    count(counter, :total, number_of_queries)

    print_iteration(counter, iteration)
  end

  @big_key "foooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
  defp query(index), do: :eredis_cluster.q(["GET", "#{@big_key}:#{index}"])

  defp count(counter, :ok), do: :counters.add(counter, 1, 1)
  defp count(counter, :error), do: :counters.add(counter, 2, 1)

  defp count(counter, :total, number_of_queries) do
    :counters.add(counter, 3, number_of_queries)
  end

  # Print functions
  defp print_iteration(counter, iteration) do
    if should_print?(iteration) do
      IO.puts("\n-> Iteration: #{iteration}")
      print_result(counter, :finish_on_errors)
      print_pool_status()
    end
  end

  # Print every 1000 loops
  @print_step 1000
  defp should_print?(i), do: rem(i, @print_step) == 0

  def print_pool_status() do
    IO.puts("-> Pool status")

    :eredis_cluster_monitor.get_all_pools()
    |> Enum.each(fn pool ->
      pool
      |> :poolboy.status()
      |> inspect()
      |> IO.puts()
    end)
  end

  defp print_result(counter, :finish_on_errors) do
    counter_result = print_result(counter)

    if counter_result[:error] > 0 do
      IO.puts("Finishing...")
      Process.exit(self(), :kill)
    end
  end

  defp print_result(counter) do
    IO.write("-> Query results: ")
    counter_result = result(counter)
    counter_result |> inspect() |> IO.puts()
    counter_result
  end
end
