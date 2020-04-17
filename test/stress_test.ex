defmodule StressTest do
  @moduledoc """
  Fires X concurrent queries per Y milliseconds.

  Setup mix.exs with eredis_cluster configuration:
  ```
    def application do
      [
        ...
        env: [{:init_nodes, [{'127.0.0.1', 30001}]}]
      ]
    end
  ```

  Start the cluster and run with `iex -S mix`:

  iex> c("test/stress_test.ex")
  iex> test = StressTest.start()
  iex> StressTest.result(test)
  iex> StressTest.finish(test)
  """

  @keys ["foo1", "foo2", "foo3", "foo4", "foo5"]
  @concurrency 100
  @interval 100
  @counter_index %{total: 1, ok: 2}

  @doc """
  Runs test for a time in milliseconds.

  Example: run for 5 seconds
  iex> test = StressTest.run_for(5000)
  """
  def run_for(time_ms, opts \\ []) do
    test = StressTest.start(opts)
    Process.sleep(time_ms)
    StressTest.finish(test)
  end

  @doc """
  Start concurrent processes that run a query per interval in milliseconds.

  iex> test = StressTest.start()
  """
  def start(opts \\ []) do
    opts = Enum.into(opts, %{concurrency: @concurrency, interval: @interval})
    counter = initialize_count()

    tasks =
      Stream.repeatedly(fn -> loop(opts.interval, counter) end)
      |> Enum.take(opts.concurrency)

    %{counter: counter, tasks: tasks}
  end

  # Sleeps between 1 and interval randomly, queries,
  # waits the full interval and repeat forever.
  defp loop(interval, counter) do
    step_size = length(@keys)

    Task.async(fn ->
      fn ->
        Task.start(fn ->
          interval
          |> :rand.uniform()
          |> Process.sleep()

          query(counter)
        end)

        :counters.add(counter, @counter_index[:total], step_size)
        Process.sleep(interval)
      end
      |> Stream.repeatedly()
      |> Stream.run()
    end)
  end

  @doc """
  Finish concurrent loops

  iex> StressTest.start(test)
  """
  def finish(state) do
    Enum.each(state.tasks, &Task.shutdown/1)
    state
  end

  @doc """
  Returns successful, pending and total number of queries ran
  since test started.

  iex> StressTest.result(test)
  """
  def result(%{counter: counter}) do
    total = :counters.get(counter, @counter_index[:total])
    ok = :counters.get(counter, @counter_index[:ok])

    [
      ok: ok,
      pending: total - ok,
      total: total
    ]
  end

  defp initialize_count() do
    Enum.each(@keys, fn key -> :eredis_cluster.q(["SET", key, 0]) end)
    :counters.new(map_size(@counter_index), [:write_concurrency])
  end

  defp query(counter) do
    increment = fn key -> :eredis_cluster.q(["INCR", key]) end

    ok_sum =
      Enum.reduce(@keys, 0, fn key, sum ->
        case increment.(key) do
          {:ok, _} -> sum + 1
          {:error, _} -> sum
        end
      end)

    :counters.add(counter, @counter_index[:ok], ok_sum)
  end
end
