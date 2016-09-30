package com.sf.stinift.exchange;

import java.util.Collections;
import java.util.List;

class BufferedFetchable implements Fetchable {
    private final Fetchable agent;
    private final int bufferSize;
    private List<Bee> buffer = Collections.emptyList();
    private int startIndex = 0;

    public BufferedFetchable(Fetchable agent, int bufferSize) {
        this.agent = agent;
        this.bufferSize = bufferSize;
    }

    private void tryFill() {
        if (size() <= 0) {
            buffer = agent.fetch(bufferSize);
            startIndex = 0;
        }
    }

    private int size() {
        return buffer.size() - startIndex;
    }

    @Override
    public Bee fetch() {
        tryFill();
        if (size() <= 0) {
            return null;
        }
        Bee bee = buffer.get(startIndex);
        startIndex++;
        return bee;
    }

    @Override
    public List<Bee> fetch(int number) {
        tryFill();
        if (size() <= 0) {
            return Collections.emptyList();
        }
        int realNum = Math.min(number, size());
        List<Bee> bees = Collections.unmodifiableList(buffer.subList(startIndex, startIndex + realNum));
        startIndex += realNum;
        return bees;
    }
}
