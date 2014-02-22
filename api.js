var ParallelPipeline = (function() {
  var { objectType, ArrayType, any, Object } = TypedObject;

  function ParallelPipeline(input, depth) {
    if (typeof depth === "undefined")
      depth = 1;

    if ((depth | 0) !== depth)
      throw new TypeError("Depth must be an integer");

    if (depth <= 0)
      throw new TypeError("Depth must be at least 1");

    var shape;
    if (depth == 1) {
      shape = [input.length];
    } else {
      var inputShape = input.shape;
      if (!inputShape || inputShape.length < depth)
        throw new TypeError("Depth too large");

      shape = [];
      for (var i = 0; i < depth; i++)
        shape.push(inputShape[i]);
    }

    assertEq(shape.length, depth);

    for (var i = 0; i < depth; i++) {
      const d = shape[i];
      if (d !== (d | 0))
        throw new TypeError("Invalid shape");
    }

    var typeObj = objectType(input);
    if (typeObj === Object) {
      if (depth !== 1)
        throw new TypeError("Depth too large");
      typeObj = any;
    } else {
      for (var i = 0; i < depth; i++)
        typeObj = typeObj.elementType;
    }

    this.depth = depth;
    this.op = new SupplyOp(input, typeObj, shape);
  }

  ParallelPipeline.prototype = {
    map: function(func) {
      return this.mapTo(this.op.grainType, func);
    },

    mapTo: function(grainType, func) {
      this.op = new MapToOp(this.op, grainType, func);
      return this;
    },

    filter: function(func) {
      if (this.depth !== 1)
        throw new TypeError("Cannot filter a pipeline unless depth is 1");
      this.op = new FilterOp(this.op, func);
      return this;
    },

    build: function() {
      return build(this.op.prepare());
    },
  };

  ///////////////////////////////////////////////////////////////////////////

  function SupplyOp(input, grainType, shape) {
    this.input = input;
    this.grainType = grainType;
    this.shape = shape;
  }

  SupplyOp.prototype = {
    prepare: function() {
      return new SupplyState(this);
    },
  };

  function SupplyState(op) {
    this.op = op;
    this.shape = this.op.shape;
    this.positions = [];
    for (var i = 0; i < this.shape.length; i++)
      this.positions.push(0);
    this.grainType = this.op.grainType;
  }

  SupplyState.prototype = {
    next: function() {
      var v = index(this.op.input, this.positions);
      increment(this.positions, this.shape);
      return v;
    },
  };

  ///////////////////////////////////////////////////////////////////////////

  function MapToOp(prevOp, grainType, func) {
    this.prevOp = prevOp;
    this.grainType = grainType;
    this.func = func;
  }

  MapToOp.prototype = {
    prepare: function() {
      return new MapState(this, this.prevOp.prepare());
    },
  };

  function MapState(op, prevState) {
    this.op = op;
    this.prevState = prevState;
    this.shape = prevState.shape;
    this.grainType = op.grainType;
  }

  MapState.prototype = {
    next: function() {
      var v = this.prevState.next();
      return this.op.func(v);
    }
  };

  ///////////////////////////////////////////////////////////////////////////

  function FilterOp(prevOp, func) {
    this.prevOp = prevOp;
    this.grainType = prevOp.grainType;
    this.func = func;
  }

  FilterOp.prototype = {
    prepare: function() {
      var prevState = this.prevOp.prepare();
      var grainType = prevState.grainType;
      var temp = build(prevState);
      var keeps = new Uint8Array(temp.length);
      var count = 0;
      for (var i = 0; i < temp.length; i++)
        if ((keeps[i] = this.func(temp[i])))
          count++;
      return new FilterState(grainType, temp, keeps, count);
    }
  };

  function FilterState(grainType, temp, keeps, count) {
    this.temp = temp;
    this.keeps = keeps;
    this.grainType = grainType;
    this.shape = [count];
    this.position = 0;
  }

  FilterState.prototype = {
    next: function() {
      while (!this.keeps[this.position])
        this.position++;
      return this.temp[this.position++];
    }
  };

  ///////////////////////////////////////////////////////////////////////////

  function lastOp(pipeline) {
    return pipeline.ops[pipeline.ops.length - 1];
  }

  function build(state) {
    var resultArray = allocArray(state.grainType, state.shape);

    var total = 1;
    var position = [];
    for (var i = 0; i < state.shape.length; i++) {
      total *= state.shape[i];
      position.push(0);
    }

    for (var i = 0; i < total; i++) {
      setIndex(resultArray, position, state.next());
      increment(position, state.shape);
    }

    return resultArray;
  }

  function index(vec, positions) {
    var v = vec;
    for (var i = 0; i < positions.length; i++)
      v = v[positions[i]];
    return v;
  }

  function setIndex(vec, positions, value) {
    var v = vec;
    for (var i = 0; i < positions.length - 1; i++)
      v = v[positions[i]];
    v[positions[i]] = value;
  }

  function increment(positions, shape) {
    for (var i = positions.length - 1; i >= 0; i--) {
      var v = ++positions[i];
      if (v < shape[i])
        return;
      positions[i] = 0;
    }
  }

  function allocArray(grainType, shape) {
    var arrayType = grainType;
    for (var i = shape.length - 1; i >= 0; i--)
      arrayType = new ArrayType(arrayType).dimension(shape[i]);
    return new arrayType();
  }

  return ParallelPipeline;
})();

// Using it:

function ParallelPipelineTests() {
  var { uint32, objectType, ArrayType, any, Object } = TypedObject;

  function test1() {
    var uints = new ArrayType(uint32);
    var input =
      new uints([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    var output =
      new ParallelPipeline(input).map(i => i + 1).map(i => i * 2).build();
    assertEq(input.length, output.length);
    for (var i = 0; i < input.length; i++)
      assertEq((input[i] + 1) * 2, output[i]);
  }
  test1();

  function test2() {
    var uints = new ArrayType(uint32);
    var input =
      new uints([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    var output =
      new ParallelPipeline(input).map(i => i + 1).filter(i => i > 5).build();
    assertArrayEq(output, [6, 7, 8, 9, 10, 11]);
  }
  test2();

  function assertArrayEq(array1, array2) {
    assertEq(array1.length, array2.length);
    for (var i = 0; i < array1.length; i++)
      assertEq(array1[i], array2[i]);
  }
}

ParallelPipelineTests();
