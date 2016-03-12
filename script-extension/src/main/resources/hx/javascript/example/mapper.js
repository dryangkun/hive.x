// (a string, b string) ->
// (x array<string>, y int, z struct<z1:long,z2:double>)

function setup(context) {

}

function map(row, context) {
    var output = context.getOutput();
    output.reset();

    var a = row.getString(0);
    var x = [];
    if (a != null) {
        x = a.split('');
    }
    var y = x.length;

    var z = output.getChildStruct(2);
    if (z != null) {
        z.reset();

        z.write(0, (new Date()).getTime());
        z.write(1, 1.1);
    }

    output.writeArray(0, x);
    output.writeInt(1, y);
    output.writeStruct(2, z);

    context.forward();
    context.forward();

}

function cleanup(context) {

}