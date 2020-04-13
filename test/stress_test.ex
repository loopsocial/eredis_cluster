defmodule StressTest do
  @moduledoc """
  Fires X concurrent queries per Y milliseconds.

  Start test
  iex> test = StressTest.start()

  iex> StressTest.result(test)
  [ok: 5600, error: 0, total: 5600, rate: 1.0]

  iex> StressTest.finish(test)
  """
  @keys ["foo1", "foo2", "foo3", "foo4"]
  @concurrent_queries_per_loop 100
  @milliseconds_per_loop 100

  def start() do
    for key <- @keys, do: :eredis_cluster.q(["SET", key, 0])
    counter = :counters.new(1, [:write_concurrency])
    {:ok, loop_pid} = Task.start(fn -> loop(counter) end)

    %{counter: counter, pid: loop_pid}
  end

  defp loop(counter) do
    Task.start(fn ->
      for _ <- 1..@concurrent_queries_per_loop, do: Task.start_link(&increase_keys/0)
      :counters.add(counter, 1, @concurrent_queries_per_loop)
    end)

    :timer.sleep(@milliseconds_per_loop)
    loop(counter)
  end

  def finish(%{counter: counter, pid: pid}) do
    Process.exit(pid, :kill)
    result(counter)
  end

  def result(%{counter: counter}) do
    result(counter)
  end

  def result(counter) do
    ok_count = get_min_key()
    total_count = :counters.get(counter, 1)

    [
      ok: ok_count,
      error: total_count - ok_count,
      total: total_count,
      rate: ok_count / total_count
    ]
  end

  defp increase_keys() do
    for key <- @keys, do: :eredis_cluster.q(["INCR", "#{key}"])
  end

  defp get_min_key() do
    @keys
    |> Enum.map(fn key ->
      {:ok, result} = :eredis_cluster.q(["GET", key])
      String.to_integer(result)
    end)
    |> Enum.min()
  end
end
