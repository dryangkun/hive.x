// (a string, b string) -> (a string, x int, y date)

var _set;
var _size;

function reduce_begin(keys, context) {
    _set = {};
    _size = 0;
}

function reduce(keys, row, context) {
    var b = row.getString(1);
    if (b != null) {
        if (_set[b] == undefined) {
            _set[b] = 1;
            _size++;
        }
    }
}

function reduce_end(keys, context) {
    var output = context.getOutput();
    output.reset();

    output.write(0, keys[0]);
    output.writeInt(1, _size);
    output.writeDate(2, (new Date).getTime());

    _set = null;

    context.forward();
}

function cleanup(context) {

}