﻿import React, { ReactNode } from "react";
import classNames from "classnames";

interface NodeSetProps {
    children?: ReactNode | ReactNode[];
    className?: string;
    color?: string;
}

export function NodeSet(props: NodeSetProps) {
    const { children, className, color } = props;

    const colorClass = color ? "bg-faded-" + color : "bg-faded-secondary";

    return <div className={classNames("node-set", colorClass, className)}>{children}</div>;
}

interface NodeSetItemProps {
    children?: ReactNode | ReactNode[];
    icon?: string;
    color?: string;

    title?: string;
}

export function NodeSetLabel(props: NodeSetItemProps) {
    const { children, icon, color, ...rest } = props;

    const colorClass = color ? "text-" + color : undefined;

    return (
        <div className="node-set-label" {...rest}>
            {icon && <i className={classNames("icon-" + icon, colorClass)} />}
            <strong className="node-set-label-name">{children}</strong>
        </div>
    );
}

export function NodeSetItem(props: NodeSetItemProps) {
    const { children, icon, color, ...rest } = props;
    const colorClass = color ? "text-" + color : undefined;

    return (
        <div className="node-set-item" {...rest}>
            {icon && <i className={classNames("icon-" + icon, colorClass)} />}
            <strong className="node-set-item-name">{children}</strong>
        </div>
    );
}