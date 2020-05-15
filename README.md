# aws-dynamodb-util

## Overview

CosmosDB
一階層目はインデックス

DynanoDB
ローカルインデックス

## react-admin

```
<Resource name="TestDB-_metadata_" {...metadata} />,

const create = props => (
    <Create {...props}>
        <SimpleForm>
            <TextInput source="id"/>
            <ArrayInput source="indexes">
                <SimpleFormIterator>
                    <TextInput />
                </SimpleFormIterator>
            </ArrayInput>
        </SimpleForm>
    </Create>
);
```
