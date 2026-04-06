/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.SortingDataInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInputFactory;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.apache.flink.shaded.curator5.com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.streaming.api.graph.StreamConfig.requiresSorting;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link StreamTask} for executing a {@link OneInputStreamOperator}. */
@Internal
public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

    @Nullable private CheckpointBarrierHandler checkpointBarrierHandler;

    private final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    public OneInputStreamTask(Environment env) throws Exception {
        super(env);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the time provider) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param env The task environment for this task.
     * @param timeProvider Optionally, a specific time provider to use.
     */
    @VisibleForTesting
    public OneInputStreamTask(Environment env, @Nullable TimerService timeProvider)
            throws Exception {
        super(env, timeProvider);
    }

    @Override
    public void init() throws Exception {
        StreamConfig configuration = getConfiguration();
        int numberOfInputs = configuration.getNumberOfNetworkInputs();

        if (numberOfInputs > 0) {
            CheckpointedInputGate inputGate = createCheckpointedInputGate();
            Counter numRecordsIn = setupNumRecordsInCounter(mainOperator);
            DataOutput<IN> output = createDataOutput(numRecordsIn);
            StreamTaskInput<IN> input = createTaskInput(inputGate);

            StreamConfig.InputConfig[] inputConfigs =
                    configuration.getInputs(getUserCodeClassLoader());
            StreamConfig.InputConfig inputConfig = inputConfigs[0];
            if (requiresSorting(inputConfig)) {
                checkState(
                        !configuration.isCheckpointingEnabled(),
                        "Checkpointing is not allowed with sorted inputs.");
                input = wrapWithSorted(input);
            }

            getEnvironment()
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .reuseRecordsInputCounter(numRecordsIn);

            inputProcessor = new StreamOneInputProcessor<>(input, output, operatorChain);
        }
        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge);
        // wrap watermark gauge since registered metrics must be unique
        getEnvironment()
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge::getValue);

        // ---- 打印 OneInputStreamTask 内部封装结构 ----
        printTaskStructure();
    }

    private void printTaskStructure() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nwill21");
        sb.append("╔══════════════════════════════════════════════════════════════╗\n");
        sb.append("║          OneInputStreamTask 内部封装结构                      ║\n");
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append(String.format("║ Task名称    : %-48s║\n", getName()));
        sb.append(String.format("║ Task类      : %-48s║\n", this.getClass().getSimpleName()));
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append("║ 封装层次 (从外到内):                                           ║\n");
        sb.append("║                                                              ║\n");

        // 第1层: OneInputStreamTask 自身
        sb.append(String.format("║  [1] %-57s║\n", this.getClass().getName()));
        sb.append("║      │                                                       ║\n");

        // 第2层: mainOperator (StreamFlatMap 等)
        if (mainOperator != null) {
            sb.append(String.format("║  [2] mainOperator: %-43s║\n",
                    mainOperator.getClass().getSimpleName()));
            sb.append("║      │   完整类名: " + padRight(mainOperator.getClass().getName(), 343) + "║\n");
            sb.append("║      │                                                       ║\n");

            // 第3层: userFunction（通过反射从 AbstractUdfStreamOperator 取）
            try {
                java.lang.reflect.Field f =
                        org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.class
                                .getDeclaredField("userFunction");
                f.setAccessible(true);
                Object userFunc = f.get(mainOperator);
                if (userFunc != null) {
                    sb.append(String.format("║  [3] userFunction: %-43s║\n",
                            userFunc.getClass().getSimpleName()));
                    sb.append("║      │   完整类名: " + padRight(userFunc.getClass().getName(), 343) + "║\n");
                    sb.append("║      │   父接口  : ");
                    for (Class<?> iface : userFunc.getClass().getInterfaces()) {
                        sb.append(iface.getSimpleName()).append(" ");
                    }
                    sb.append("\n");

                    // 打印 userFunction 声明的方法
                    sb.append("║      │   实现方法:\n");
                    for (java.lang.reflect.Method m : userFunc.getClass().getDeclaredMethods()) {
                        sb.append(String.format("║      │     - %s(%s)\n",
                                m.getName(),
                                java.util.Arrays.stream(m.getParameterTypes())
                                        .map(Class::getSimpleName)
                                        .collect(java.util.stream.Collectors.joining(", "))));
                    }
                } else {
                    sb.append("║  [3] userFunction: null (非 UDF 算子)                       ║\n");
                }
            } catch (Exception e) {
                sb.append("║  [3] userFunction: (非 AbstractUdfStreamOperator，无 UDF)     ║\n");
            }
        } else {
            sb.append("║  [2] mainOperator: null                                      ║\n");
        }

        sb.append("║                                                              ║\n");
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");

        // 打印 OperatorChain 中所有 chained operator
        sb.append("║ OperatorChain 中的全部算子:                                    ║\n");
        int[] idx = {1};
        operatorChain.getAllOperators(false).forEach(wrapper -> {
            org.apache.flink.streaming.api.operators.StreamOperator<?> op =
                    wrapper.getStreamOperator();
            sb.append(String.format("║   [%d] %-56s║\n", idx[0]++, op.getClass().getSimpleName()));
            try {
                java.lang.reflect.Field f =
                        org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.class
                                .getDeclaredField("userFunction");
                f.setAccessible(true);
                Object uf = f.get(op);
                if (uf != null) {
                    sb.append(String.format("║        └─ UDF: %-47s║\n",
                            uf.getClass().getName()));
                }
            } catch (Exception ignored) {}
        });

        sb.append("╚══════════════════════════════════════════════════════════════╝\n");
        System.out.println(sb);
    }

    private static String padRight(String s, int n) {
        if (s.length() >= n) return s.substring(0, n - 3) + "...";
        return String.format("%-" + n + "s", s);
    }

    @Override
    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.ofNullable(checkpointBarrierHandler);
    }

    private StreamTaskInput<IN> wrapWithSorted(StreamTaskInput<IN> input) {
        ClassLoader userCodeClassLoader = getUserCodeClassLoader();
        return new SortingDataInput<>(
                input,
                configuration.getTypeSerializerIn(input.getInputIndex(), userCodeClassLoader),
                configuration.getStateKeySerializer(userCodeClassLoader),
                configuration.getStatePartitioner(input.getInputIndex(), userCodeClassLoader),
                getEnvironment().getMemoryManager(),
                getEnvironment().getIOManager(),
                getExecutionConfig().isObjectReuseEnabled(),
                configuration.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        getEnvironment().getTaskConfiguration(),
                        userCodeClassLoader),
                getJobConfiguration(),
                this,
                getExecutionConfig());
    }

    @SuppressWarnings("unchecked")
    private CheckpointedInputGate createCheckpointedInputGate() {
        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();

        checkpointBarrierHandler =
                InputProcessorUtil.createCheckpointBarrierHandler(
                        this,
                        configuration,
                        getCheckpointCoordinator(),
                        getTaskNameWithSubtaskAndId(),
                        new List[] {Arrays.asList(inputGates)},
                        Collections.emptyList(),
                        mainMailboxExecutor,
                        systemTimerService);

        CheckpointedInputGate[] checkpointedInputGates =
                InputProcessorUtil.createCheckpointedMultipleInputGate(
                        mainMailboxExecutor,
                        new List[] {Arrays.asList(inputGates)},
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        checkpointBarrierHandler,
                        configuration);

        return Iterables.getOnlyElement(Arrays.asList(checkpointedInputGates));
    }

    private DataOutput<IN> createDataOutput(Counter numRecordsIn) {
        return new StreamTaskNetworkOutput<>(
                operatorChain.getFinishedOnRestoreInputOrDefault(mainOperator),
                inputWatermarkGauge,
                numRecordsIn);
    }

    private StreamTaskInput<IN> createTaskInput(CheckpointedInputGate inputGate) {
        int numberOfInputChannels = inputGate.getNumberOfInputChannels();
        StatusWatermarkValve statusWatermarkValve = new StatusWatermarkValve(numberOfInputChannels);

        TypeSerializer<IN> inSerializer =
                configuration.getTypeSerializerIn1(getUserCodeClassLoader());

        return StreamTaskNetworkInputFactory.create(
                inputGate,
                inSerializer,
                getEnvironment().getIOManager(),
                statusWatermarkValve,
                0,
                getEnvironment().getTaskStateManager().getInputRescalingDescriptor(),
                gateIndex ->
                        configuration
                                .getInPhysicalEdges(getUserCodeClassLoader())
                                .get(gateIndex)
                                .getPartitioner(),
                getEnvironment().getTaskInfo());
    }

    /**
     * The network data output implementation used for processing stream elements from {@link
     * StreamTaskNetworkInput} in one input processor.
     */
    private static class StreamTaskNetworkOutput<IN> implements DataOutput<IN> {

        private final Input<IN> operator;

        private final WatermarkGauge watermarkGauge;
        private final Counter numRecordsIn;

        private StreamTaskNetworkOutput(
                Input<IN> operator, WatermarkGauge watermarkGauge, Counter numRecordsIn) {

            this.operator = checkNotNull(operator);
            this.watermarkGauge = checkNotNull(watermarkGauge);
            this.numRecordsIn = checkNotNull(numRecordsIn);
        }

        @Override
        public void emitRecord(StreamRecord<IN> record) throws Exception {
            numRecordsIn.inc();
            operator.setKeyContextElement(record);
            operator.processElement(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
            operator.processWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            operator.processWatermarkStatus(watermarkStatus);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            operator.processLatencyMarker(latencyMarker);
        }
    }
}
