import React from "react";
import AceEditor from "react-ace";

import {
    Card,
    Alert,
    CardHeader,
    CardBody,
    DropdownMenu,
    DropdownToggle,
    DropdownItem,
    UncontrolledDropdown,
    Row,
    Col,
    Button,
} from "reactstrap";

import { Icon } from "components/common/Icon";
import AboutView from "components/common/AboutView";
import "./AdminJsConsole.scss";

export default function AdminJSConsole() {
    return (
        <div className="content-margin">
            <Row>
                <Col xxl={9}>
                    <Row>
                        <Col>
                            <h2>
                                <Icon icon="administrator-js-console" /> Admin JS Console
                            </h2>
                        </Col>
                        <Col sm={"auto"}>
                            <AboutView>
                                <Row>
                                    <Col sm={"auto"}>
                                        <Icon
                                            className="fs-1"
                                            icon="administrator-js-console"
                                            color="info"
                                            margin="m-0"
                                        />
                                    </Col>
                                    <Col>
                                        <p>
                                            <strong>Admin JS Console</strong> is a specialized feature primarily
                                            intended for resolving server errors. It provides a direct interface to the
                                            underlying system, granting the capacity to execute scripts for intricate
                                            server operations.
                                        </p>
                                        <p>
                                            It is predominantly intended for advanced troubleshooting and rectification
                                            procedures executed by system administrators or RavenDB support.
                                        </p>
                                        <hr />
                                        <div className="small-label mb-2">useful links</div>
                                        <a href="https://ravendb.net/docs/article-page/5.4/csharp/studio/server/debug/admin-js-console#console-view">
                                            <Icon icon="newtab" /> Docs - Admin JS Console
                                        </a>
                                    </Col>
                                </Row>
                            </AboutView>
                        </Col>
                    </Row>

                    <Alert color="warning hstack gap-4">
                        <div className="flex-shrink-0">
                            <Icon icon="warning" /> WARNING
                        </div>
                        <div>
                            Do not use the console unless you are sure about what you're doing. Running a script in the
                            Admin Console could cause your server to crash, cause loss of data, or other irreversible
                            harm.
                        </div>
                    </Alert>

                    <Card>
                        <CardHeader className="hstack gap-4">
                            <h3 className="m-0">Script target</h3>
                            <UncontrolledDropdown>
                                <DropdownToggle caret>Server</DropdownToggle>
                                <DropdownMenu>
                                    <DropdownItem>
                                        <Icon icon="server" /> Server
                                    </DropdownItem>
                                    <DropdownItem divider />
                                    <DropdownItem>
                                        <Icon icon="database" />
                                        DbName 1
                                    </DropdownItem>
                                    <DropdownItem>
                                        <Icon icon="database" />
                                        DbName 2
                                    </DropdownItem>
                                    <DropdownItem>
                                        <Icon icon="database" />
                                        DbName 3
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                            <div className="text-info">
                                Accessible within the script under <code>server</code> variable
                            </div>
                        </CardHeader>
                        <CardBody>
                            <div className="admin-js-console-grid">
                                <div>
                                    <h3>Script</h3>
                                </div>
                                <div>
                                    {" "}
                                    <AceEditor
                                        mode="java"
                                        theme="github"
                                        onChange={null}
                                        name="UNIQUE_ID_OF_DIV"
                                        editorProps={{ $blockScrolling: true }}
                                        value={null}
                                        fontSize={14}
                                        showPrintMargin={true}
                                        showGutter={true}
                                        highlightActiveLine={true}
                                        setOptions={{
                                            enableBasicAutocompletion: true,
                                            enableLiveAutocompletion: true,
                                            enableSnippets: true,
                                            showLineNumbers: true,
                                            tabSize: 2,
                                        }}
                                        width="100%"
                                        height="300px"
                                    />
                                </div>
                                <div className="align-self-end">
                                    <Button color="primary" size="lg" className="px-4 py-2">
                                        <Icon icon="play" className="fs-1 d-inline-block" margin="mb-2" />
                                        <div className="kbd">
                                            <kbd>ctrl</kbd> <strong>+</strong> <kbd>enter</kbd>
                                        </div>
                                    </Button>
                                </div>
                                <div>
                                    <h3>Script result</h3>
                                </div>
                                <div>
                                    <AceEditor
                                        mode="java"
                                        theme="github"
                                        onChange={null}
                                        name="UNIQUE_ID_OF_DIV"
                                        editorProps={{ $blockScrolling: true }}
                                        value={null}
                                        fontSize={14}
                                        showPrintMargin={true}
                                        showGutter={true}
                                        highlightActiveLine={true}
                                        setOptions={{
                                            enableBasicAutocompletion: true,
                                            enableLiveAutocompletion: true,
                                            enableSnippets: true,
                                            showLineNumbers: true,
                                            tabSize: 2,
                                        }}
                                        width="100%"
                                        height="400px"
                                    />
                                </div>
                            </div>
                        </CardBody>
                    </Card>
                </Col>
            </Row>
        </div>
    );
}
