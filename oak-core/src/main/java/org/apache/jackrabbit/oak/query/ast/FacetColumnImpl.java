package org.apache.jackrabbit.oak.query.ast;

import org.apache.jackrabbit.oak.api.PropertyValue;

/**
 *
 */
public class FacetColumnImpl extends ColumnImpl {
    public FacetColumnImpl(String selectorName, String propertyName, String columnName) {
        super(selectorName, propertyName, columnName);
    }

    @Override
    public SelectorImpl getSelector() {
        return super.getSelector();
    }

    @Override
    public void bindSelector(SourceImpl source) {
        super.bindSelector(source);
    }

    @Override
    public PropertyValue currentProperty() {
        return this.getSelector().currentOakProperty(getPropertyName());
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    boolean accept(AstVisitor v) {
        return super.accept(v);
    }

    @Override
    public String getPropertyName() {
        return super.getPropertyName();
    }

    @Override
    public String getColumnName() {
        return super.getColumnName();
    }
}
