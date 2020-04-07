defmodule StressTest do
  def infinity_loop() do
    counter = :counters.new(3, [:write_concurrency])

    with {:ok, pid} <- Task.start(__MODULE__, :loop, [counter, 0]) do
      %{counter: counter, pid: pid}
    end
  end

  def loop(counter, iteration, continue \\ true)

  def loop(_counter, _iteration, false) do
    IO.puts("Stopped!")
  end

  def loop(counter, iteration, true) do
    start_query_task(counter, iteration)
    continue = has_error?(counter, iteration)

    loop(counter, iteration + 1, continue)
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
    print_result(counter)
    print_pool_status()
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
      print_result(counter)
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

  defp print_result(counter) do
    IO.write("-> Query results: ")
    counter |> result() |> inspect() |> IO.puts()
  end

  # Every 100 iterations, return false if any error
  defp has_error?(counter, iteration) do
    if rem(iteration, 100) == 0 do
      result(counter)[:error] == 0
    else
      true
    end
  end
end
