package com.actio;

import java.util.List;
import java.util.Objects;

/**
 * Created by dimitarpopov on 27/10/2015.
 */

class WrapColumns {
    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    private List<String> columns;
    private final int keyColumn=1;

    public WrapColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> unwrap() {
        return this.columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WrapColumns that = (WrapColumns) o;
        return Objects.equals(columns.get(keyColumn), that.columns.get(keyColumn));
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns.get(keyColumn));
    }
}
