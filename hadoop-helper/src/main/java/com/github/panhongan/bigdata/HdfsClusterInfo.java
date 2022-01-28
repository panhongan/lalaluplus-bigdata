package com.github.panhongan.bigdata;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author lalaluplus
 * @since 2022.1.20
 */

@Getter
@Setter
@ToString
class HdfsClusterInfo {

    private String nameService;

    private NameNodeInfo nameNodeInfo1;

    private NameNodeInfo nameNodeInfo2;
}
